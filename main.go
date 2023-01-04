package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sourcegraph/log"

	oldfreeport "github.com/phayes/freeport"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	freeport "github.com/slimsag/freeport"
)

var (
	flagListen            = flag.String("listen", ":8080", "HTTP address to listen on")
	flagWorkers           = flag.Int("workers", 8, "number of worker subprocesses to spawn")
	flagTimeout           = flag.Duration("timeout", 10*time.Second, "if request to worker takes longer than this, it will be killed")
	flagTimeoutHeader     = flag.String("header", "X-Stabilize-Timeout", "request header used to override default timeout value, if not an empty string")
	flagConcurrency       = flag.Int("concurrency", 10, "number of concurrent requests to allow per worker")
	flagPrometheus        = flag.String("prometheus", ":6060", "publish Prometheus metrics on specified address")
	flagPrometheusAppName = flag.String("prometheus-app-name", "", "App name to specify in Prometheus")

	flagDemo       = flag.Bool("demo", false, "start an HTTP demo server that does nothing")
	flagDemoListen = flag.String("demo-listen", ":9700", "specify HTTP address for demo server to listen on")
)

type worker struct {
	// log is a logger that carries the worker's pid and port as a fields
	log log.Logger

	ctx    context.Context
	port   int
	cancel func()
	pid    int
	cmd    *exec.Cmd
	output *io.PipeReader
	done   chan struct{}
}

// watch monitors the worker until it dies.
func (w *worker) watch() {
	go func() {
		<-w.ctx.Done()

		// Kill the process.
		if err := w.cmd.Process.Kill(); err != nil {
			if err != nil {
				w.log.Error("killing process", log.Error(err))
			}
		}

		// Also kill subprocesses (OS X, Linux) -- not supported on Windows.
		pgid, err := syscall.Getpgid(w.pid)
		if err == nil {
			syscall.Kill(-pgid, 15)
		}

		w.cmd.ProcessState, _ = w.cmd.Process.Wait()
		close(w.done)
		w.output.Close()
	}()

	output := bufio.NewReader(w.output)
	for {
		line, err := output.ReadString('\n')
		w.log.Info(line)
		if err != nil {
			w.log.Error("read error",
				log.Error(err),
				log.String("process.state", w.cmd.ProcessState.String()))
			return
		}
	}
}

// spawnWorker spawns a new worker process. stderr and stdout will be logged,
// the done channel signals when the worker has died, and w.cancel() can be
// used to kill the worker.
func spawnWorker(ctx context.Context, logger log.Logger, port int, command string, args ...string) *worker {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// Create a new process group so any subprocesses the worker spawns can
		// be killed.
		Setpgid: true,
	}
	pr, pw := io.Pipe()
	cmd.Stderr = pw
	cmd.Stdout = pw
	w := &worker{
		log: logger.With(log.Int("port", port)),

		ctx:    ctx,
		port:   port,
		cancel: cancel,
		cmd:    cmd,
		output: pr,
		done:   make(chan struct{}),
	}

	if err := cmd.Start(); err != nil {
		logger.Error("spawn error", log.Error(err))
		close(w.done)
		return w
	}

	// Track the process ID associated with this worker
	w.pid = w.cmd.Process.Pid
	w.log = w.log.With(log.Int("pid", w.pid))

	go w.watch()

	w.log.Info("started")
	return w
}

type stabilizer struct {
	log     log.Logger
	command string
	args    []string

	workerPool     chan *worker
	workerByPortMu sync.RWMutex
	workerByPort   map[int]*worker
}

func templateArgs(args []string, port string) []string {
	var v []string
	for _, arg := range args {
		v = append(v, strings.Replace(arg, "{{.Port}}", port, -1))
	}
	return v
}

func (s *stabilizer) acquire() *worker {
	for {
		w := <-s.workerPool
		if w.ctx.Err() == nil {
			return w
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *stabilizer) release(w *worker) {
	go func() {
		s.workerPool <- w
	}()
}

func getFreePort() (port int, err error) {
	if v, _ := strconv.ParseBool(os.Getenv("USE_OLD_FREEPORT")); v == true {
		return oldfreeport.GetFreePort()
	}
	return freeport.GetFreePort()
}

// ensureWorkers ensures that n workers are always alive. If they die, they
// will be started again.
func (s *stabilizer) ensureWorkers(n int) {
	s.log.Info("ensuring workers",
		log.String("command", strings.Join(append([]string{s.command}, s.args...), " ")),
		log.Int("count", n))

	for i := 0; i < n; i++ {
		go func(i int) {
			for {
				workerPort, err := getFreePort()
				if err != nil {
					s.log.Warn("failed to find free port")
					time.Sleep(1 * time.Second)
					continue
				}

				args := templateArgs(s.args, fmt.Sprint(workerPort))
				w := spawnWorker(context.Background(),
					log.Scoped("worker", "worker instance"),
					workerPort, s.command, args...)
				s.workerByPortMu.Lock()
				s.workerByPort[workerPort] = w
				s.workerByPortMu.Unlock()
				var (
					done        bool
					poolEntries int
				)
				for {
					if done {
						break
					}
					if poolEntries < *flagConcurrency {
						select {
						case s.workerPool <- w:
							poolEntries++
						case <-w.done:
							done = true
						}
						continue
					}
					<-w.done
					break
				}
			}
		}(i)
	}
}

func (s *stabilizer) director(req *http.Request) {
	timeout := *flagTimeout
	if *flagTimeoutHeader != "" {
		var err error
		timeout, err = time.ParseDuration(req.Header.Get(*flagTimeoutHeader))
		if err != nil {
			timeout = *flagTimeout
		}
	}

	// We cannot cancel this timeout effectively within http.ReverseProxy director
	ctx, _ := context.WithTimeout(req.Context(), timeout)
	*req = *req.WithContext(ctx)

	// Pull a worker from the pool and set it as our target.
	worker := s.acquire()
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%v", worker.port))
	s.log.Debug("handling request",
		log.String("url", req.URL.String()),
		log.String("target", target.String()))

	// Copy what httputil.NewSingleHostReverseProxy would do.
	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
	req.URL.Path = path.Join(target.Path, req.URL.Path)
	if target.RawQuery == "" || req.URL.RawQuery == "" {
		req.URL.RawQuery = target.RawQuery + req.URL.RawQuery
	} else {
		req.URL.RawQuery = target.RawQuery + "&" + req.URL.RawQuery
	}
	if _, ok := req.Header["User-Agent"]; !ok {
		// explicitly disable User-Agent so it's not set to default value
		req.Header.Set("User-Agent", "")
	}
}

var workerRestartsCounter prometheus.Counter

func main() {
	flag.Parse()

	liblog := log.Init(log.Resource{
		Name:       *flagPrometheusAppName,
		InstanceID: hostname(),
		Version:    "",
	})
	defer liblog.Sync()

	workerRestartsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: *flagPrometheusAppName + "_hss_worker_restarts",
		Help: "The total number of worker process restarts",
	})

	if *flagDemo {
		demoLog := log.Scoped("demo", "demo endpoint")

		demoLog.Info("listening", log.String("addr", *flagDemoListen))
		rand.Seed(time.Now().UnixNano())
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if rand.Int()%2 == 0 {
				demoLog.Warn("stuck")
				i := 0
				for {
					// Pretend the server OS thread has gotten completely stuck in a loop.
					i = i + 1
					if false {
						fmt.Println(i)
					}
				}
			}
			fmt.Fprintf(w, "Hello from worker %s\n", *flagDemoListen)
		})

		if err := http.ListenAndServe(*flagDemoListen, nil); err != nil {
			demoLog.Fatal("server exited", log.Error(err))
		}
	}

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(2)
	}

	if *flagPrometheus != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(*flagPrometheus, mux)
		}()
	}

	s := &stabilizer{
		log:          log.Scoped("stabilizer", "worker stabilizer"),
		command:      flag.Arg(0),
		args:         flag.Args()[1:],
		workerPool:   make(chan *worker, *flagWorkers**flagConcurrency),
		workerByPort: make(map[int]*worker),
	}
	go s.ensureWorkers(*flagWorkers)

	handler := &httputil.ReverseProxy{
		Director: s.director,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   2000 * time.Millisecond,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		ModifyResponse: func(r *http.Response) error {
			// Set the X-Worker response header for debugging purposes.
			workerPort, _ := strconv.ParseInt(r.Request.URL.Port(), 10, 64)
			s.workerByPortMu.RLock()
			w := s.workerByPort[int(workerPort)]
			s.workerByPortMu.RUnlock()
			s.release(w)
			r.Header.Set("X-Worker", fmt.Sprint(w.pid))
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, r *http.Request, err error) {
			// Set the X-Worker response header for debugging purposes.
			workerPort, _ := strconv.ParseInt(r.URL.Port(), 10, 64)
			s.workerByPortMu.RLock()
			w := s.workerByPort[int(workerPort)]
			s.workerByPortMu.RUnlock()
			s.release(w)
			rw.Header().Set("X-Worker", fmt.Sprint(w.pid))

			rw.WriteHeader(http.StatusServiceUnavailable)
			// If the request timed out, kill the worker since it may be stuck.
			// It will automatically restart.
			if ctxErr := r.Context().Err(); ctxErr != nil {
				w.log.Warn("restarting due to timeout", log.String("ctxErr", ctxErr.Error()))
				workerRestartsCounter.Inc()
				w.cancel()
				_ = json.NewEncoder(rw).Encode(&map[string]interface{}{
					"error": fmt.Sprintf("worker %v: restarted due to timeout", w.pid),
					"code":  "hss_worker_timeout",
				})
				return
			}

			// Technically we could hit other errors here if e.g. communication
			// between our reverse proxy and the worker was failing for some
			// other reason like the network being flooded, but in practice
			// this is unlikely to happen and instead the most likely case is
			// that the worker was killed due to another request on the same
			// worker timing out. In this case, having a different error code
			// to handle is not that useful so we also return
			// hss_worker_timeout.
			w.log.Error("error encountered", log.Error(err))
			_ = json.NewEncoder(rw).Encode(&map[string]interface{}{
				"error": fmt.Sprintf("worker %v: %v", w.pid, err),
				"code":  "hss_worker_timeout",
			})
		},
	}
	if err := http.ListenAndServe(*flagListen, handler); err != nil {
		log.Scoped("server", "").Fatal("server exited", log.Error(err))
	}
}
