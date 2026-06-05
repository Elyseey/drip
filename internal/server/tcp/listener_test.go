package tcp

import (
	"net"
	"testing"
	"time"

	"drip/internal/shared/pool"
	"drip/internal/shared/recovery"
	"go.uber.org/zap"
)

func TestListenerStopDoesNotHangWhenWorkerPoolRejectsConnection(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}

	logger := zap.NewNop()
	workerPool := pool.NewWorkerPool(1, 1)
	workerPool.Close()

	listener := &Listener{
		listener:   ln,
		stopCh:     make(chan struct{}),
		connSem:    make(chan struct{}, maxConns),
		workerPool: workerPool,
		logger:     logger,
		recoverer:  recovery.NewRecoverer(logger, recovery.NewPanicMetrics(logger, nil)),
	}

	listener.wg.Add(1)
	go listener.acceptLoop()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial failed: %v", err)
	}
	_ = clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- listener.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("listener.Stop() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listener.Stop() hung after worker pool rejected a connection")
	}
}
