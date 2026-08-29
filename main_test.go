package main

import (
	"context"
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
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
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
			go http.Get("http://" + listener.Addr().String()) //nolint:errcheck
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
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want deadline exceeded", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !stopped {
				t.Fatal("workers were not stopped during forced shutdown")
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
		fields := strings.Fields(string(data))
		if len(fields) < 3 || fields[2] != "Z" {
			t.Fatalf("worker child %d remains running: %s", pid, data)
		}
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("checking worker child %d: %v", pid, err)
	}
}
