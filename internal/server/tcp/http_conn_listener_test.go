package tcp

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestConnQueueListenerEnqueueAfterCloseIsRejected(t *testing.T) {
	t.Parallel()

	ln := newConnQueueListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, 8)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	if ln.Enqueue(server) {
		t.Fatal("Enqueue succeeded after Close")
	}
}

func TestConnQueueListenerCloseDrainsQueuedConns(t *testing.T) {
	t.Parallel()

	ln := newConnQueueListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, 8)
	server, client := net.Pipe()
	defer client.Close()

	if !ln.Enqueue(server) {
		t.Fatal("Enqueue failed before Close")
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	// Drained connections are closed; subsequent reads should fail quickly.
	_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := server.Read(buf); err == nil {
		t.Fatal("expected drained connection to be closed")
	}
}

func TestConnQueueListenerEnqueueCloseRace(t *testing.T) {
	t.Parallel()

	ln := newConnQueueListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, 64)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server, client := net.Pipe()
			defer client.Close()
			if !ln.Enqueue(server) {
				_ = server.Close()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = ln.Close()
	}()

	wg.Wait()

	// After Close completes, no connection should remain queued forever.
	select {
	case conn := <-ln.conns:
		t.Fatalf("found queued connection after Close: %v", conn)
	default:
	}
}
