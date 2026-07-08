package proxy

import (
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"drip/internal/server/tunnel"
	"drip/internal/shared/protocol"

	"go.uber.org/zap"
)

func TestOpenStreamTimeoutClosesLateStream(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	manager := tunnel.NewManagerWithConfig(logger, tunnel.ManagerConfig{
		MaxTunnels:      10,
		MaxTunnelsPerIP: 10,
		RateLimit:       1000,
		RateLimitWindow: time.Second,
	})

	subdomain, err := manager.Register(nil, "open-stream")
	if err != nil {
		t.Fatalf("register tunnel: %v", err)
	}
	t.Cleanup(func() {
		manager.Unregister(subdomain)
	})

	tconn, ok := manager.Get(subdomain)
	if !ok {
		t.Fatalf("registered tunnel %q was not found", subdomain)
	}
	tconn.SetTunnelType(protocol.TunnelTypeHTTP)

	var closed atomic.Bool
	tconn.SetOpenStream(func() (net.Conn, error) {
		// Block past the caller timeout, then return a live stream that must be closed.
		time.Sleep(50 * time.Millisecond)
		serverSide, clientSide := net.Pipe()
		go func() {
			defer clientSide.Close()
			buf := make([]byte, 1)
			_, _ = clientSide.Read(buf)
		}()
		return &closeTrackingConn{
			Conn: serverSide,
			onClose: func() {
				closed.Store(true)
			},
		}, nil
	})

	handler := NewHandler(HandlerConfig{
		Manager:      manager,
		Logger:       logger,
		ServerDomain: "drip.test",
		TunnelDomain: testTunnelDomain,
	})

	stream, err := handler.openStream(tconn, 10*time.Millisecond)
	if stream != nil {
		_ = stream.Close()
		t.Fatal("expected nil stream on timeout")
	}
	if err == nil || !strings.Contains(err.Error(), "open stream timeout") {
		t.Fatalf("expected open stream timeout, got %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for !closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !closed.Load() {
		t.Fatal("timed-out stream was not closed")
	}
}

type closeTrackingConn struct {
	net.Conn
	onClose func()
}

func (c *closeTrackingConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return c.Conn.Close()
}
