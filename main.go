package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	"os/signal"
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
	flagPreShutdownPause  = flag.Duration("pre-shutdown-pause", 0, "time to continue accepting requests after SIGTERM before shutting down")
	flagGracefulTimeout   = flag.Duration("graceful-shutdown-timeout", 10*time.Second, "maximum total time for the pre-shutdown pause and in-flight request drain")

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
	output *os.File
	done   chan struct{}
}

func (w *worker) killProcessGroup() {
	// Setpgid makes the leader PID the process group ID. Address the group by
	// that stable ID even if the leader has already exited and been reaped.
	if err := syscall.Kill(-w.pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		w.log.Error("killing process group", log.Error(err))
	}
}

func (w *worker) reapProcessGroup() {
	if err := reapProcessGroup(w.pid); err != nil {
		w.log.Error("reaping process group", log.Error(err))
	}
}

// watch monitors the worker until it dies.
func (w *worker) watch() {
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		output := bufio.NewReader(w.output)
		for {
			line, err := output.ReadString('\n')
			if line != "" {
				w.log.Info(strings.TrimSuffix(line, "\n"))
			}
			if err != nil {
				if err != io.EOF && !errors.Is(err, os.ErrClosed) && w.ctx.Err() == nil {
					w.log.Error("reading process output", log.Error(err))
				}
				return
			}
		}
	}()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- w.cmd.Wait()
	}()

	select {
	case <-w.ctx.Done():
		w.killProcessGroup()
		err := <-waitDone
		w.reapProcessGroup()
		if err != nil {
			w.log.Debug("worker stopped", log.Error(err))
		}
	case err := <-waitDone:
		// A worker leader can exit while descendants remain alive. Cancel its
		// routing context and terminate every remaining group member.
		w.cancel()
		w.killProcessGroup()
		w.reapProcessGroup()
		if err != nil {
			w.log.Warn("worker exited", log.Error(err))
		}
	}
	w.output.Close()
	<-outputDone
	close(w.done)
}

// spawnWorker spawns a new worker process. stderr and stdout will be logged,
// the done channel signals when the worker has died, and w.cancel() can be
// used to kill the worker.
func spawnWorker(ctx context.Context, logger log.Logger, port int, command string, args ...string) *worker {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.Command(command, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// Create a new process group so any subprocesses the worker spawns can
		// be killed.
		Setpgid: true,
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		logger.Error("creating worker output pipe", log.Error(err))
		cancel()
		done := make(chan struct{})
		close(done)
		return &worker{ctx: ctx, cancel: cancel, done: done}
	}
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
		cancel()
		pw.Close()
		pr.Close()
		close(w.done)
		return w
	}
	// The child inherited the write descriptor. The parent must not retain it,
	// otherwise worker output never reaches EOF.
	pw.Close()

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

	workerPool chan *worker
}

type workerContextKey struct{}

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
func (s *stabilizer) ensureWorkers(ctx context.Context, wg *sync.WaitGroup, n int) {
	s.log.Info("ensuring workers",
		log.String("command", strings.Join(append([]string{s.command}, s.args...), " ")),
		log.Int("count", n))

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				workerPort, err := getFreePort()
				if err != nil {
					s.log.Warn("failed to find free port")
					select {
					case <-time.After(1 * time.Second):
					case <-ctx.Done():
						return
					}
					continue
				}

				args := templateArgs(s.args, fmt.Sprint(workerPort))
				w := spawnWorker(ctx,
					log.Scoped("worker", "worker instance"),
					workerPort, s.command, args...)
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
						case <-ctx.Done():
							<-w.done
							return
						}
						continue
					}
					select {
					case <-w.done:
					case <-ctx.Done():
						<-w.done
						return
					}
					break
				}
			}
		}(i)
	}
}

var errForcedShutdown = fmt.Errorf("shutdown forced")

const workerCleanupTimeout = 5 * time.Second

func validateShutdownConfig(preShutdownPause, gracefulTimeout time.Duration) error {
	if preShutdownPause < 0 {
		return fmt.Errorf("pre-shutdown pause must not be negative")
	}
	if gracefulTimeout < 0 {
		return fmt.Errorf("graceful shutdown timeout must not be negative")
	}
	if gracefulTimeout <= preShutdownPause {
		return fmt.Errorf("graceful shutdown timeout must be greater than pre-shutdown pause")
	}
	return nil
}

func serveUntilShutdown(
	logger log.Logger,
	server *http.Server,
	listener net.Listener,
	stopWorkers func(),
	waitWorkers func(),
	signals <-chan os.Signal,
	preShutdownPause time.Duration,
	gracefulTimeout time.Duration,
) (returnErr error) {
	defer func() {
		stopWorkers()
		workersDone := make(chan struct{})
		go func() {
			waitWorkers()
			close(workersDone)
		}()
		select {
		case <-workersDone:
		case <-time.After(workerCleanupTimeout):
			logger.Error("worker cleanup deadline reached")
			if returnErr == nil {
				returnErr = fmt.Errorf("worker cleanup deadline reached")
			}
		}
		if returnErr == nil {
			logger.Info("graceful shutdown complete")
		}
	}()

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	var shutdownStarted time.Time
	select {
	case err := <-serveDone:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case sig := <-signals:
		shutdownStarted = time.Now()
		logger.Info("shutdown signal received; continuing to accept requests during pre-shutdown pause",
			log.String("signal", sig.String()),
			log.Duration("pause", preShutdownPause),
			log.Duration("gracefulTimeout", gracefulTimeout))
	}

	shutdownCtx, cancelShutdown := context.WithDeadline(context.Background(), shutdownStarted.Add(gracefulTimeout))
	defer cancelShutdown()
	pause := time.NewTimer(preShutdownPause)
	defer pause.Stop()
	select {
	case sig := <-signals:
		logger.Warn("second shutdown signal received during pre-shutdown pause; forcing shutdown", log.String("signal", sig.String()))
		_ = server.Close()
		return errForcedShutdown
	case <-shutdownCtx.Done():
		logger.Warn("graceful shutdown deadline reached during pre-shutdown pause; forcing shutdown", log.Error(shutdownCtx.Err()))
		_ = server.Close()
		return nil
	case <-pause.C:
	}

	logger.Info("pre-shutdown pause complete; refusing new connections and draining in-flight requests")
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()

	var shutdownErr error
	select {
	case shutdownErr = <-shutdownDone:
		if shutdownErr != nil {
			logger.Warn("graceful shutdown deadline reached; forcing shutdown", log.Error(shutdownErr))
			_ = server.Close()
			if errors.Is(shutdownErr, context.DeadlineExceeded) {
				shutdownErr = nil
			}
		}
	case sig := <-signals:
		logger.Warn("second shutdown signal received while draining; forcing shutdown", log.String("signal", sig.String()))
		_ = server.Close()
		shutdownErr = errForcedShutdown
	}

	return shutdownErr
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
	*req = *req.WithContext(context.WithValue(req.Context(), workerContextKey{}, worker))
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
	if err := validateShutdownConfig(*flagPreShutdownPause, *flagGracefulTimeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

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
	if err := enableChildSubreaper(); err != nil {
		log.Scoped("server", "").Fatal("enabling child process reaper", log.Error(err))
	}
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	if *flagPrometheus != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(*flagPrometheus, mux)
		}()
	}

	s := &stabilizer{
		log:        log.Scoped("stabilizer", "worker stabilizer"),
		command:    flag.Arg(0),
		args:       flag.Args()[1:],
		workerPool: make(chan *worker, *flagWorkers**flagConcurrency),
	}
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	var workerWG sync.WaitGroup
	s.ensureWorkers(workerCtx, &workerWG, *flagWorkers)

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
			w := r.Request.Context().Value(workerContextKey{}).(*worker)
			s.release(w)
			r.Header.Set("X-Worker", fmt.Sprint(w.pid))
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, r *http.Request, err error) {
			// Set the X-Worker response header for debugging purposes.
			w := r.Context().Value(workerContextKey{}).(*worker)
			s.release(w)
			rw.Header().Set("X-Worker", fmt.Sprint(w.pid))

			rw.WriteHeader(http.StatusServiceUnavailable)

			// This error type matches what Rocket uses (the Rust server
			// we use in syntect server)
			type Err struct {
				// HTTP error code
				Code int `json:"code"`
				// Error string that can be matched on
				Reason string `json:"reason"`
				// PII-safe human-readable description, which can be used for logging
				Description string `json:"description"`
			}

			// If the request timed out, kill the worker since it may be stuck.
			// It will automatically restart.
			if ctxErr := r.Context().Err(); ctxErr != nil {
				w.log.Warn("restarting due to timeout", log.String("ctxErr", ctxErr.Error()))
				workerRestartsCounter.Inc()
				w.cancel()
				_ = json.NewEncoder(rw).Encode(&map[string]interface{}{
					"error": Err{
						Code:        http.StatusServiceUnavailable,
						Reason:      "hss_worker_timeout",
						Description: fmt.Sprintf("Worker (pid: %v) failed to highlight file; restarting it", w.pid),
					},
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
				"error": Err{
					Code:        http.StatusServiceUnavailable,
					Reason:      "hss_worker_unknown_error",
					Description: fmt.Sprintf("Worker (pid: %v) unknown error: %v", w.pid, err),
				},
			})
		},
	}
	listener, err := net.Listen("tcp", *flagListen)
	if err != nil {
		log.Scoped("server", "").Fatal("failed to listen", log.Error(err))
	}
	server := &http.Server{Handler: handler}
	if err := serveUntilShutdown(
		log.Scoped("server", "HTTP server"),
		server,
		listener,
		stopWorkers,
		workerWG.Wait,
		signals,
		*flagPreShutdownPause,
		*flagGracefulTimeout,
	); err != nil {
		log.Scoped("server", "").Error("server exited", log.Error(err))
		os.Exit(1)
	}
}
