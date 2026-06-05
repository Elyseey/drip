package proxy

import (
	"bufio"
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"drip/internal/server/tunnel"
	"drip/internal/shared/httputil"
	"drip/internal/shared/netutil"
	"drip/internal/shared/pool"
	"drip/internal/shared/protocol"
	"drip/internal/shared/qos"
	"drip/internal/shared/utils"
)

// bufio.Reader pool to reduce allocations on hot path
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024)
	},
}

// bufioWriter pool for HTTP request forwarding
var bufioWriterPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 32*1024)
	},
}

const openStreamTimeout = 3 * time.Second

type HandlerConfig struct {
	Manager             *tunnel.Manager
	Logger              *zap.Logger
	ServerDomain        string
	TunnelDomain        string
	AuthToken           string
	MetricsToken        string
	MaxRequestBodyBytes int64
}

type Handler struct {
	manager             *tunnel.Manager
	logger              *zap.Logger
	serverDomain        string
	tunnelDomain        string
	authToken           string
	metricsToken        string
	publicPort          int
	maxRequestBodyBytes int64

	// WebSocket tunnel support
	wsUpgrader    websocket.Upgrader
	wsConnHandler WSConnectionHandler

	// Server capabilities
	allowedTransports  []string
	allowedTunnelTypes []string
}

// WSConnectionHandler handles WebSocket tunnel connections
type WSConnectionHandler interface {
	HandleWSConnection(conn net.Conn, remoteAddr string)
}

func NewHandler(cfg HandlerConfig) *Handler {
	serverDomain := cfg.ServerDomain
	tunnelDomain := cfg.TunnelDomain
	return &Handler{
		manager:             cfg.Manager,
		logger:              cfg.Logger,
		serverDomain:        serverDomain,
		tunnelDomain:        tunnelDomain,
		authToken:           cfg.AuthToken,
		metricsToken:        cfg.MetricsToken,
		maxRequestBodyBytes: cfg.MaxRequestBodyBytes,
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  256 * 1024,
			WriteBufferSize: 256 * 1024,
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true // Non-browser clients may not send Origin
				}
				originURL, err := url.Parse(origin)
				if err != nil {
					return false
				}
				originHost := originURL.Host
				if originHost == "" {
					originHost = originURL.Path
				}
				// Allow requests from server domain or tunnel domain
				if originHost == serverDomain || originHost == tunnelDomain {
					return true
				}
				// Allow subdomains of tunnel domain
				if tunnelDomain != "" && strings.HasSuffix(originHost, "."+tunnelDomain) {
					return true
				}
				return false
			},
		},
	}
}

// SetWSConnectionHandler sets the handler for WebSocket tunnel connections
func (h *Handler) SetWSConnectionHandler(handler WSConnectionHandler) {
	h.wsConnHandler = handler
}

// SetPublicPort sets the public port for URL generation
func (h *Handler) SetPublicPort(port int) {
	h.publicPort = port
}

// SetAllowedTransports sets the allowed transport protocols
func (h *Handler) SetAllowedTransports(transports []string) {
	h.allowedTransports = transports
}

// SetAllowedTunnelTypes sets the allowed tunnel types
func (h *Handler) SetAllowedTunnelTypes(types []string) {
	h.allowedTunnelTypes = types
}

// IsTransportAllowed checks if a transport is allowed
func (h *Handler) IsTransportAllowed(transport string) bool {
	return utils.ContainsIgnoreCase(h.allowedTransports, transport)
}

// IsTunnelTypeAllowed checks if a tunnel type is allowed
func (h *Handler) IsTunnelTypeAllowed(tunnelType string) bool {
	return utils.ContainsIgnoreCase(h.allowedTunnelTypes, tunnelType)
}

// GetPreferredTransport returns the preferred transport for auto-detection
func (h *Handler) GetPreferredTransport() string {
	if len(h.allowedTransports) == 0 {
		return "tcp"
	}
	if len(h.allowedTransports) == 1 {
		return h.allowedTransports[0]
	}
	return "tcp"
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Discovery endpoint for client auto-detection
	if r.URL.Path == "/_drip/discover" {
		h.serveDiscovery(w, r)
		return
	}

	// WebSocket tunnel endpoint - must be checked before other routes
	if r.URL.Path == "/_drip/ws" {
		h.handleTunnelWebSocket(w, r)
		return
	}

	if r.URL.Path == "/health" {
		h.serveHealth(w, r)
		return
	}
	if r.URL.Path == "/stats" {
		h.serveStats(w, r)
		return
	}
	if r.URL.Path == "/metrics" {
		h.serveMetrics(w, r)
		return
	}

	subdomain, result := h.extractSubdomain(r.Host)
	switch result {
	case subdomainHome:
		h.serveHomePage(w, r)
		return
	case subdomainNotFound:
		h.serveTunnelNotFound(w, r)
		return
	}

	tconn, ok := h.manager.Get(subdomain)
	if !ok || tconn == nil {
		h.serveTunnelNotFound(w, r)
		return
	}
	if tconn.IsClosed() {
		http.Error(w, "Tunnel connection closed", http.StatusBadGateway)
		return
	}

	if tconn.HasIPAccessControl() {
		clientIP := netutil.ExtractClientIP(r)
		if !tconn.IsIPAllowed(clientIP) {
			http.Error(w, "Access denied: your IP is not allowed", http.StatusForbidden)
			return
		}
	}

	if auth := tconn.GetProxyAuth(); auth != nil && auth.Enabled {
		clientIP := netutil.ExtractClientIP(r)

		if authLimiter.isRateLimited(clientIP) {
			w.Header().Set("Retry-After", "60")
			http.Error(w, "Too many failed authentication attempts. Please try again later.", http.StatusTooManyRequests)
			return
		}

		if isBearerProxyAuth(auth) {
			if !h.isBearerAuthenticated(r, auth) {
				authLimiter.recordFailure(clientIP)
				h.serveBearerAuthRequired(w, "drip")
				return
			}
			authLimiter.resetFailures(clientIP)
		} else {
			if r.URL.Path == "/_drip/login" {
				h.handleProxyLoginWithRateLimit(w, r, tconn, subdomain, clientIP)
				return
			}
			if !h.isProxyAuthenticated(r, subdomain) {
				h.serveLoginPage(w, r, subdomain, "")
				return
			}
		}
	}

	tType := tconn.GetTunnelType()
	if tType != "" && tType != protocol.TunnelTypeHTTP && tType != protocol.TunnelTypeHTTPS {
		http.Error(w, "Tunnel does not accept HTTP traffic", http.StatusBadGateway)
		return
	}

	if r.Method == http.MethodConnect {
		http.Error(w, "CONNECT not supported for HTTP tunnels", http.StatusMethodNotAllowed)
		return
	}

	if h.isWebSocketUpgrade(r) {
		h.handleWebSocket(w, r, tconn)
		return
	}

	if h.maxRequestBodyBytes > 0 && r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxRequestBodyBytes)
	}

	stream, err := h.openStreamWithTimeout(tconn)
	if err != nil {
		httputil.SetCloseConnection(w)
		http.Error(w, "Tunnel unavailable", http.StatusBadGateway)
		return
	}
	defer stream.Close()

	tconn.IncActiveConnections()
	defer tconn.DecActiveConnections()

	var limitedStream net.Conn = stream
	if limiter := tconn.GetLimiter(); limiter != nil && limiter.IsLimited() {
		if l, ok := limiter.(*qos.Limiter); ok {
			limitedStream = qos.NewLimitedConn(r.Context(), stream, l)
		}
	}

	countingStream := netutil.NewCountingConn(limitedStream,
		tconn.AddBytesOut,
		tconn.AddBytesIn,
	)

	// Use pooled bufio.Writer to batch small writes and reduce syscalls
	bw := bufioWriterPool.Get().(*bufio.Writer)
	bw.Reset(countingStream)
	if err := r.Write(bw); err != nil {
		bufioWriterPool.Put(bw)
		httputil.SetCloseConnection(w)
		_ = r.Body.Close()
		http.Error(w, "Forward failed", http.StatusBadGateway)
		return
	}
	if err := bw.Flush(); err != nil {
		bufioWriterPool.Put(bw)
		httputil.SetCloseConnection(w)
		_ = r.Body.Close()
		http.Error(w, "Forward flush failed", http.StatusBadGateway)
		return
	}
	bufioWriterPool.Put(bw)

	reader := bufioReaderPool.Get().(*bufio.Reader)
	reader.Reset(countingStream)
	resp, err := http.ReadResponse(reader, r)
	if err != nil {
		bufioReaderPool.Put(reader)
		httputil.SetCloseConnection(w)
		http.Error(w, "Read response failed", http.StatusBadGateway)
		return
	}
	defer func() {
		_ = resp.Body.Close()
		bufioReaderPool.Put(reader)
	}()

	h.copyResponseHeaders(w.Header(), resp.Header, r.Host)

	statusCode := resp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	if r.Method == http.MethodHead || statusCode == http.StatusNoContent || statusCode == http.StatusNotModified {
		if resp.ContentLength >= 0 {
			httputil.SetContentLength(w, resp.ContentLength)
		} else {
			w.Header().Del("Content-Length")
		}
		w.WriteHeader(statusCode)
		return
	}

	streamingResponse := httputil.IsEventStream(resp.Header)
	if streamingResponse {
		w.Header().Del("Content-Length")
	} else if resp.ContentLength >= 0 {
		httputil.SetContentLength(w, resp.ContentLength)
	} else {
		w.Header().Del("Content-Length")
	}

	w.WriteHeader(statusCode)

	// Copy with context cancellation support using AfterFunc (avoids per-request goroutine)
	stop := context.AfterFunc(r.Context(), func() { _ = stream.Close() })
	defer stop()

	if streamingResponse {
		flushResponse(w)
		_, _ = copyResponseBodyFlushing(w, resp.Body)
		return
	}

	// Use pooled buffer for zero-copy optimization
	buf := pool.GetBuffer(pool.SizeLarge)
	defer pool.PutBuffer(buf)

	_, _ = io.CopyBuffer(w, resp.Body, (*buf)[:])
}

type streamResult struct {
	stream net.Conn
	err    error
}

func (h *Handler) openStreamWithTimeout(tconn *tunnel.Connection) (net.Conn, error) {
	return h.openStream(tconn, openStreamTimeout)
}

func (h *Handler) openStream(tconn *tunnel.Connection, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan streamResult)

	go func() {
		s, err := tconn.OpenStream()
		select {
		case ch <- streamResult{s, err}:
		case <-ctx.Done():
			if s != nil {
				_ = s.Close()
			}
		}
	}()

	select {
	case r := <-ch:
		return r.stream, r.err
	case <-time.After(timeout):
		cancel()
		return nil, fmt.Errorf("open stream timeout")
	}
}

func (h *Handler) copyResponseHeaders(dst http.Header, src http.Header, proxyHost string) {
	for key, values := range src {
		canonicalKey := http.CanonicalHeaderKey(key)

		// Hop-by-hop headers must not be forwarded.
		if canonicalKey == "Connection" ||
			canonicalKey == "Keep-Alive" ||
			canonicalKey == "Transfer-Encoding" ||
			canonicalKey == "Upgrade" ||
			canonicalKey == "Proxy-Connection" ||
			canonicalKey == "Te" ||
			canonicalKey == "Trailer" {
			continue
		}

		if canonicalKey == "Location" && len(values) > 0 {
			dst.Set("Location", h.rewriteLocationHeader(values[0], proxyHost))
			continue
		}

		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func (h *Handler) rewriteLocationHeader(location, proxyHost string) string {
	if !strings.HasPrefix(location, "http://") && !strings.HasPrefix(location, "https://") {
		return location
	}

	locationURL, err := url.Parse(location)
	if err != nil {
		return location
	}

	if locationURL.Host == "localhost" ||
		strings.HasPrefix(locationURL.Host, "localhost:") ||
		locationURL.Host == "127.0.0.1" ||
		strings.HasPrefix(locationURL.Host, "127.0.0.1:") {
		rewritten := fmt.Sprintf("https://%s%s", proxyHost, locationURL.Path)
		if locationURL.RawQuery != "" {
			rewritten += "?" + locationURL.RawQuery
		}
		if locationURL.Fragment != "" {
			rewritten += "#" + locationURL.Fragment
		}
		return rewritten
	}

	return location
}

type subdomainResult int

const (
	subdomainHome subdomainResult = iota
	subdomainFound
	subdomainNotFound
)

func (h *Handler) extractSubdomain(host string) (string, subdomainResult) {
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	if host == h.serverDomain {
		return "", subdomainHome
	}

	suffix := "." + h.tunnelDomain
	if strings.HasSuffix(host, suffix) {
		return strings.TrimSuffix(host, suffix), subdomainFound
	}

	if host == h.tunnelDomain {
		return "", subdomainNotFound
	}

	return "", subdomainNotFound
}

func (h *Handler) validateMetricsAuth(w http.ResponseWriter, r *http.Request, realm string) bool {
	if h.metricsToken == "" {
		return true
	}

	token := extractBearerToken(r.Header.Get("Authorization"))

	if subtle.ConstantTimeCompare([]byte(token), []byte(h.metricsToken)) != 1 {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s"`, realm))
		http.Error(w, "Unauthorized: provide metrics token via 'Authorization: Bearer <token>' header", http.StatusUnauthorized)
		return false
	}

	return true
}
