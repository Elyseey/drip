package tcp

import (
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"drip/internal/shared/protocol"
	"drip/internal/shared/stats"

	"go.uber.org/zap"
)

// TransportType defines the transport protocol for tunnel connections
type TransportType string

const (
	// TransportAuto automatically selects transport based on server address
	TransportAuto TransportType = "auto"
	// TransportTCP uses direct TLS 1.3 connection
	TransportTCP TransportType = "tcp"
	// TransportWebSocket uses WebSocket over TLS (CDN-friendly)
	TransportWebSocket TransportType = "wss"
)

type LatencyCallback func(latency time.Duration)

type ConnectorConfig struct {
	ServerAddr string
	Token      string
	TunnelType protocol.TunnelType
	LocalHost  string
	LocalPort  int
	Subdomain  string
	Insecure   bool

	PoolSize int
	PoolMin  int
	PoolMax  int

	AllowIPs []string
	DenyIPs  []string

	// Proxy authentication
	AuthPass   string
	AuthBearer string

	// Transport protocol selection
	Transport TransportType

	// Bandwidth limit (bytes/sec), 0 = unlimited
	Bandwidth int64

	// SkipLocalTLSVerify disables certificate verification for local HTTPS backends.
	SkipLocalTLSVerify bool
}

type TunnelClient interface {
	Connect() error
	Close() error
	Wait()
	GetURL() string
	GetSubdomain() string
	SetLatencyCallback(cb LatencyCallback)
	GetLatency() time.Duration
	GetStats() *stats.TrafficStats
	IsClosed() bool
}

func NewTunnelClient(cfg *ConnectorConfig, logger *zap.Logger) TunnelClient {
	return NewPoolClient(cfg, logger)
}

func isExpectedCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "session shutdown") ||
		strings.Contains(s, "connection reset")
}
