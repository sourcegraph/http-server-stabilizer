package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sourcegraph/log"
)

func TestMain(m *testing.M) {
	if err := enableChildSubreaper(); err != nil {
		fmt.Fprintf(os.Stderr, "enable child subreaper: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestValidateShutdownConfig(t *testing.T) {
	for _, test := range []struct {
		name             string
		preShutdownPause time.Duration
		gracefulTimeout  time.Duration
		wantErr          string
	}{
		{name: "valid", preShutdownPause: 5 * time.Second, gracefulTimeout: 15 * time.Second},
		{name: "negative pause", preShutdownPause: -time.Second, gracefulTimeout: 15 * time.Second, wantErr: "pre-shutdown pause must not be negative"},
		{name: "negative timeout", gracefulTimeout: -time.Second, wantErr: "graceful shutdown timeout must not be negative"},
		{name: "timeout equals pause", preShutdownPause: 5 * time.Second, gracefulTimeout: 5 * time.Second, wantErr: "graceful shutdown timeout must be greater than pre-shutdown pause"},
		{name: "timeout less than pause", preShutdownPause: 5 * time.Second, gracefulTimeout: 4 * time.Second, wantErr: "graceful shutdown timeout must be greater than pre-shutdown pause"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateShutdownConfig(test.preShutdownPause, test.gracefulTimeout)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("validateShutdownConfig: %v", err)
				}
				return
			}
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("validateShutdownConfig error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestServeUntilShutdownDrainsRequests(t *testing.T) {
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(slowStarted)
			<-releaseSlow
		}
		fmt.Fprint(w, r.URL.Path)
	}))
	defer backend.Close()

	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: httputil.NewSingleHostReverseProxy(target)}
	signals := make(chan os.Signal, 2)
	workersStopped := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- serveUntilShutdown(
			log.Scoped("test", "test"), server, listener,
			func() { close(workersStopped) }, func() {}, signals,
			100*time.Millisecond, time.Second,
		)
	}()

	client := &http.Client{}
	slowDone := make(chan string, 1)
	go func() {
		resp, err := client.Get("http://" + listener.Addr().String() + "/slow")
		if err != nil {
			slowDone <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			slowDone <- "error: " + err.Error()
			return
		}
		slowDone <- string(body)
	}()
	<-slowStarted
	signals <- syscall.SIGTERM

	// Requests continue to be accepted during the endpoint-propagation pause.
	resp, err := client.Get("http://" + listener.Addr().String() + "/during-pause")
	if err != nil {
		t.Fatalf("request during pause: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := string(body); got != "/during-pause" {
		t.Fatalf("request during pause returned %q", got)
	}

	// Once the pause ends, Shutdown closes the listener and refuses new work.
	deadline := time.Now().Add(time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 20*time.Millisecond)
		if dialErr != nil {
			break
		}
		conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("listener still accepted connections after pre-shutdown pause")
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-workersStopped:
		t.Fatal("workers stopped before the in-flight request completed")
	default:
	}
	close(releaseSlow)
	if got := <-slowDone; got != "/slow" {
		t.Fatalf("in-flight request did not complete: %s", got)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("serveUntilShutdown: %v", err)
	}
	select {
	case <-workersStopped:
	default:
		t.Fatal("workers were not stopped after HTTP drain")
	}
}

func TestServeUntilShutdownForcesOnDeadlineAndSecondSignal(t *testing.T) {
	for _, test := range []struct {
		name             string
		secondSignal     bool
		preShutdownPause time.Duration
	}{
		{name: "deadline while draining"},
		{name: "deadline during pause", preShutdownPause: time.Second},
		{name: "second signal during pause", secondSignal: true, preShutdownPause: time.Second},
		{name: "second signal while draining", secondSignal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			handlerDone := make(chan struct{})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				close(started)
				<-r.Context().Done()
			})}
			signals := make(chan os.Signal, 2)
			var stopped bool
			var mu sync.Mutex
			runDone := make(chan error, 1)
			go func() {
				runDone <- serveUntilShutdown(
					log.Scoped("test", "test"), server, listener,
					func() { mu.Lock(); stopped = true; mu.Unlock() }, func() {}, signals,
					test.preShutdownPause, 50*time.Millisecond,
				)
			}()
			clientDone := make(chan error, 1)
			go func() {
				resp, err := http.Get("http://" + listener.Addr().String())
				if resp != nil {
					resp.Body.Close()
				}
				clientDone <- err
			}()
			<-started
			signals <- syscall.SIGTERM
			if test.secondSignal {
				time.Sleep(10 * time.Millisecond)
				signals <- syscall.SIGTERM
			}
			err = <-runDone
			if test.secondSignal {
				if !errors.Is(err, errForcedShutdown) {
					t.Fatalf("got %v, want forced shutdown", err)
				}
			} else if err != nil {
				t.Fatalf("got %v, want successful deadline shutdown", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !stopped {
				t.Fatal("workers were not stopped during forced shutdown")
			}
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("active handler did not stop after forced shutdown")
			}
			select {
			case err := <-clientDone:
				if err == nil {
					t.Fatal("client request unexpectedly completed during forced shutdown")
				}
			case <-time.After(time.Second):
				t.Fatal("client request did not stop after forced shutdown")
			}
		})
	}
}

func TestWorkerCancellationKillsProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	ctx, cancel := context.WithCancel(context.Background())
	w := spawnWorker(ctx, log.Scoped("test-worker", "test worker"), 0,
		"sh", "-c", "sleep 30 & echo $! > "+pidFile+"; wait")

	childPID := readPID(t, pidFile)
	cancel()
	select {
	case <-w.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after cancellation")
	}
	assertProcessNotRunning(t, childPID)
}

func TestWorkerLeaderExitKillsProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	w := spawnWorker(context.Background(), log.Scoped("test-worker", "test worker"), 0,
		"sh", "-c", "sleep 30 & echo $! > "+pidFile)

	childPID := readPID(t, pidFile)
	select {
	case <-w.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after its leader exited")
	}
	if w.ctx.Err() == nil {
		t.Fatal("naturally exited worker retained a live routing context")
	}
	assertProcessNotRunning(t, childPID)
}

func TestAcquireRejectsExitedWorker(t *testing.T) {
	deadCtx, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	dead := &worker{ctx: deadCtx}
	live := &worker{ctx: context.Background()}
	s := &stabilizer{workerPool: make(chan *worker, 2)}
	s.workerPool <- dead
	s.workerPool <- live

	if got := s.acquire(); got != live {
		t.Fatal("acquired an exited worker")
	}
}

func TestDirectorStoresSelectedWorkerOnRequest(t *testing.T) {
	w := &worker{ctx: context.Background(), port: 1234}
	s := &stabilizer{
		log:        log.Scoped("test", "test"),
		workerPool: make(chan *worker, 1),
	}
	s.workerPool <- w
	req := httptest.NewRequest(http.MethodGet, "http://stabilizer/request", nil)
	req = req.WithContext(context.WithValue(req.Context(), handlingContextKey{}, context.Background()))

	s.director(req)

	if got := req.Context().Value(workerContextKey{}); got != w {
		t.Fatalf("request worker = %p, want %p", got, w)
	}
}

// startTestProxy runs the stabilizer's reverse proxy in front of one fake
// worker served by handler. The worker is not added to the pool; tests add
// pool entries themselves. Canceling the worker only cancels its context.
func startTestProxy(t *testing.T, handler http.Handler) (string, *worker, *stabilizer) {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(backendURL.Port())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{log: log.Scoped("test-worker", "test worker"), ctx: ctx, cancel: cancel, port: port, pid: 1234}
	s := &stabilizer{log: log.Scoped("test", "test"), workerPool: make(chan *worker, 100)}
	proxy := httptest.NewServer(s.reverseProxy())
	t.Cleanup(proxy.Close)
	return proxy.URL, w, s
}

type proxyResult struct {
	status int
	body   string
	err    error
}

func postThroughProxy(ctx context.Context, proxyURL, stabilizeTimeout string) proxyResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL, strings.NewReader("body"))
	if err != nil {
		return proxyResult{err: err}
	}
	if stabilizeTimeout != "" {
		req.Header.Set("X-Stabilize-Timeout", stabilizeTimeout)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return proxyResult{err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return proxyResult{status: resp.StatusCode, body: string(body), err: err}
}

func errorReason(t *testing.T, r proxyResult) string {
	t.Helper()
	if r.err != nil {
		t.Fatalf("request failed: %v", r.err)
	}
	var body struct {
		Error struct {
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(r.body), &body); err != nil {
		t.Fatalf("response %d %q is not an HSS error: %v", r.status, r.body, err)
	}
	return body.Error.Reason
}

func waitForWorkerRestart(t *testing.T, w *worker) {
	t.Helper()
	select {
	case <-w.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("worker was not restarted")
	}
}

func TestCallerCancellationDoesNotRestartWorker(t *testing.T) {
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(rw, "ok")
	}))
	s.workerPool <- w
	s.workerPool <- w

	// One caller gives up while a concurrent request is in flight on the
	// same worker.
	healthy := make(chan proxyResult, 1)
	go func() { healthy <- postThroughProxy(context.Background(), proxyURL, "5s") }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if r := postThroughProxy(ctx, proxyURL, "5s"); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("canceled request = %+v, want caller deadline error", r)
	}

	if r := <-healthy; r.err != nil || r.status != http.StatusOK || r.body != "ok" {
		t.Fatalf("concurrent request = %+v, want 200 ok", r)
	}
	select {
	case <-w.ctx.Done():
		t.Fatal("healthy worker was restarted because a caller gave up")
	case <-time.After(400 * time.Millisecond): // let the canceled request's worker call finish
	}
}

func TestStuckWorkerRestartsWhenCallerGivesUpFirst(t *testing.T) {
	done := make(chan struct{})
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select { // stuck until HSS abandons the request
		case <-r.Context().Done():
		case <-done:
		}
	}))
	t.Cleanup(func() { close(done) })
	s.workerPool <- w

	// The caller's own timeout is shorter than HSS's, as with Sourcegraph's
	// frontend; HSS must still detect and replace the stuck worker.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if r := postThroughProxy(ctx, proxyURL, "200ms"); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("request = %+v, want caller deadline error", r)
	}
	waitForWorkerRestart(t, w)
}

func TestStabilizationTimeoutRestartsWorker(t *testing.T) {
	done := make(chan struct{})
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-done:
		}
	}))
	t.Cleanup(func() { close(done) })
	s.workerPool <- w

	r := postThroughProxy(context.Background(), proxyURL, "50ms")
	if reason := errorReason(t, r); r.status != http.StatusServiceUnavailable || reason != "hss_worker_timeout" {
		t.Fatalf("response = %d %q, want 503 hss_worker_timeout", r.status, reason)
	}
	waitForWorkerRestart(t, w)
}

func TestStabilizationTimeoutStartsAfterWorkerIsAcquired(t *testing.T) {
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(rw, "ok")
	}))
	go func() {
		// The request waits for a free worker for longer than its timeout.
		time.Sleep(200 * time.Millisecond)
		s.workerPool <- w
	}()

	if r := postThroughProxy(context.Background(), proxyURL, "100ms"); r.err != nil || r.status != http.StatusOK {
		t.Fatalf("response = %+v, want 200", r)
	}
	if w.ctx.Err() != nil {
		t.Fatal("worker was restarted for time spent waiting to acquire it")
	}
}

func TestNonPositiveStabilizeTimeoutUsesDefault(t *testing.T) {
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(rw, "ok")
	}))
	for _, header := range []string{"0", "0s", "-1s"} {
		s.workerPool <- w
		if r := postThroughProxy(context.Background(), proxyURL, header); r.err != nil || r.status != http.StatusOK {
			t.Fatalf("X-Stabilize-Timeout %q: response = %+v, want 200", header, r)
		}
	}
	if w.ctx.Err() != nil {
		t.Fatal("non-positive X-Stabilize-Timeout restarted the worker")
	}
}

func TestRequestOnRestartingWorkerReportsTimeout(t *testing.T) {
	var w *worker
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		// Another request's timeout stops the worker while this request is in
		// flight, so the connection closes without a response.
		w.cancel()
		if conn, _, err := rw.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	}))
	s.workerPool <- w

	r := postThroughProxy(context.Background(), proxyURL, "5s")
	if reason := errorReason(t, r); r.status != http.StatusServiceUnavailable || reason != "hss_worker_timeout" {
		t.Fatalf("response = %d %q, want 503 hss_worker_timeout", r.status, reason)
	}
}

func TestProxyUsesFreshBackendConnectionPerRequest(t *testing.T) {
	var mu sync.Mutex
	clientAddrs := map[string]bool{}
	proxyURL, w, s := startTestProxy(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		clientAddrs[r.RemoteAddr] = true
		mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		rw.Write(body)
	}))

	// A pooled connection can be closed by the worker (idle timeout, restart)
	// just as it is reused, which net/http cannot retry for a POST. Each
	// request must therefore use its own backend connection.
	const requests = 10
	for i := 0; i < requests; i++ {
		s.workerPool <- w
		if r := postThroughProxy(context.Background(), proxyURL, ""); r.err != nil || r.status != http.StatusOK || r.body != "body" {
			t.Fatalf("response = %+v, want 200 body", r)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(clientAddrs) != requests {
		t.Fatalf("backend connections = %d, want one per request (%d)", len(clientAddrs), requests)
	}
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker subprocess did not write child PID")
	return 0
}

func assertProcessNotRunning(t *testing.T, pid int) {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err == nil {
		t.Fatalf("worker child %d was not reaped: %s", pid, data)
	}
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("checking worker child %d: %v", pid, err)
	}
}
