# SSE Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix GitHub issue #25 so Server-Sent Events are delivered promptly through HTTP/HTTPS drip tunnels.

**Architecture:** Keep the existing split proxy architecture: `internal/server/proxy` handles public HTTP responses, and `internal/client/tcp` forwards requests to the local backend. Add a shared SSE content-type detector, a server-side flushing copy branch for SSE bodies, and client-side write-deadline behavior that does not interrupt long-lived SSE responses.

**Tech Stack:** Go 1.26, `net/http`, `net.Pipe`, `httptest`, existing drip tunnel/proxy packages.

---

## File Structure

- Create `internal/shared/httputil/streaming.go`: shared SSE response detection from HTTP headers.
- Create `internal/shared/httputil/streaming_test.go`: unit coverage for `text/event-stream` media type parsing.
- Create `internal/server/proxy/streaming.go`: small server-side helper for flushing streaming response bodies.
- Create `internal/server/proxy/handler_sse_test.go`: regression tests for public HTTP tunnel SSE behavior through `Handler`.
- Modify `internal/server/proxy/handler.go`: branch response body copying when the response is SSE.
- Create `internal/client/tcp/pool_handler_test.go`: regression test for client-side SSE forwarding without a lingering short write deadline.
- Modify `internal/client/tcp/pool_handler.go`: avoid per-chunk short write deadlines for SSE responses.

## Task 1: Shared SSE Detection

**Files:**
- Create: `internal/shared/httputil/streaming.go`
- Create: `internal/shared/httputil/streaming_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/shared/httputil/streaming_test.go`:

```go
package httputil

import (
	"net/http"
	"testing"
)

func TestIsEventStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		want        bool
	}{
		{
			name:        "plain event stream",
			contentType: "text/event-stream",
			want:        true,
		},
		{
			name:        "event stream with charset",
			contentType: "text/event-stream; charset=utf-8",
			want:        true,
		},
		{
			name:        "event stream with mixed case",
			contentType: "Text/Event-Stream; Charset=UTF-8",
			want:        true,
		},
		{
			name:        "json is not event stream",
			contentType: "application/json",
			want:        false,
		},
		{
			name:        "empty content type",
			contentType: "",
			want:        false,
		},
		{
			name:        "invalid parameter still matches media type",
			contentType: "text/event-stream; charset",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			header := http.Header{}
			if tt.contentType != "" {
				header.Set("Content-Type", tt.contentType)
			}

			if got := IsEventStream(header); got != tt.want {
				t.Fatalf("IsEventStream(%q) = %v, want %v", tt.contentType, got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./internal/shared/httputil -run TestIsEventStream -count=1
```

Expected: FAIL with `undefined: IsEventStream`.

- [ ] **Step 3: Add the shared detector**

Create `internal/shared/httputil/streaming.go`:

```go
package httputil

import (
	"mime"
	"net/http"
	"strings"
)

const eventStreamMediaType = "text/event-stream"

// IsEventStream reports whether headers describe a Server-Sent Events response.
func IsEventStream(headers http.Header) bool {
	contentType := headers.Get("Content-Type")
	if contentType == "" {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	}

	return strings.EqualFold(mediaType, eventStreamMediaType)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
go test ./internal/shared/httputil -run TestIsEventStream -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/shared/httputil/streaming.go internal/shared/httputil/streaming_test.go
git commit -m "feat: detect SSE responses"
```

## Task 2: Server-Side SSE Flush Branch

**Files:**
- Create: `internal/server/proxy/streaming.go`
- Create: `internal/server/proxy/handler_sse_test.go`
- Modify: `internal/server/proxy/handler.go`

- [ ] **Step 1: Write the failing proxy regression tests**

Create `internal/server/proxy/handler_sse_test.go`:

```go
package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"drip/internal/server/tunnel"
	"drip/internal/shared/protocol"

	"go.uber.org/zap"
)

const testTunnelDomain = "tunnels.test"

func newProxySSETestServer(t *testing.T, streamHandler func(net.Conn)) *httptest.Server {
	t.Helper()

	logger := zap.NewNop()
	manager := tunnel.NewManagerWithConfig(logger, tunnel.ManagerConfig{
		MaxTunnels:      10,
		MaxTunnelsPerIP: 10,
		RateLimit:       1000,
		RateLimitWindow: time.Second,
	})

	subdomain, err := manager.Register(nil, "demo")
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
	tconn.SetOpenStream(func() (net.Conn, error) {
		serverSide, clientSide := net.Pipe()
		go streamHandler(clientSide)
		return serverSide, nil
	})

	handler := NewHandler(HandlerConfig{
		Manager:      manager,
		Logger:       logger,
		ServerDomain: "drip.test",
		TunnelDomain: testTunnelDomain,
	})

	return httptest.NewServer(handler)
}

func readProxyRequest(t *testing.T, conn net.Conn) {
	t.Helper()

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		t.Errorf("read proxied request: %v", err)
		return
	}
	_, _ = io.Copy(io.Discard, req.Body)
	_ = req.Body.Close()
}

func doProxyRequestWithin(t *testing.T, server *httptest.Server, path string, timeout time.Duration) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Host = "demo." + testTunnelDomain

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := server.Client().Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	select {
	case resp := <-respCh:
		return resp
	case err := <-errCh:
		t.Fatalf("proxy request failed: %v", err)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for proxy response headers")
	}

	return nil
}

func readBodyWithin(t *testing.T, body io.Reader, size int, timeout time.Duration) string {
	t.Helper()

	type readResult struct {
		data []byte
		err  error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		buf := make([]byte, size)
		_, err := io.ReadFull(body, buf)
		resultCh <- readResult{data: buf, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("read response body: %v", result.err)
		}
		return string(result.data)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for response body")
	}

	return ""
}

func TestHandlerFlushesEventStreamResponse(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once

	server := newProxySSETestServer(t, func(conn net.Conn) {
		defer conn.Close()
		readProxyRequest(t, conn)
		_, _ = fmt.Fprint(conn,
			"HTTP/1.1 200 OK\r\n"+
				"Content-Type: text/event-stream; charset=utf-8\r\n"+
				"Content-Length: 999\r\n"+
				"Cache-Control: no-cache\r\n"+
				"\r\n"+
				"data: first\n\n")
		<-release
	})
	defer func() {
		releaseOnce.Do(func() { close(release) })
		server.Close()
	}()

	resp := doProxyRequestWithin(t, server, "/events", time.Second)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Length"); got != "" {
		t.Fatalf("Content-Length = %q, want empty for SSE", got)
	}

	got := readBodyWithin(t, resp.Body, len("data: first\n\n"), 500*time.Millisecond)
	if got != "data: first\n\n" {
		t.Fatalf("body prefix = %q, want first SSE event", got)
	}

	releaseOnce.Do(func() { close(release) })
}

func TestHandlerPreservesContentLengthForOrdinaryResponse(t *testing.T) {
	server := newProxySSETestServer(t, func(conn net.Conn) {
		defer conn.Close()
		readProxyRequest(t, conn)
		_, _ = fmt.Fprint(conn,
			"HTTP/1.1 200 OK\r\n"+
				"Content-Type: text/plain\r\n"+
				"Content-Length: 5\r\n"+
				"\r\n"+
				"hello")
	})
	defer server.Close()

	resp := doProxyRequestWithin(t, server, "/plain", time.Second)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read ordinary response body: %v", err)
	}
	if got := resp.Header.Get("Content-Length"); got != "5" {
		t.Fatalf("Content-Length = %q, want 5", got)
	}
	if string(body) != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
}
```

- [ ] **Step 2: Run the proxy tests to verify the SSE test fails**

Run:

```bash
go test ./internal/server/proxy -run 'TestHandler(FlushesEventStreamResponse|PreservesContentLengthForOrdinaryResponse)' -count=1
```

Expected: FAIL in `TestHandlerFlushesEventStreamResponse` with `timed out waiting for proxy response headers` or `timed out waiting for response body`.

- [ ] **Step 3: Add the server streaming helper**

Create `internal/server/proxy/streaming.go`:

```go
package proxy

import (
	"io"
	"net/http"

	"drip/internal/shared/pool"
)

func flushResponse(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func copyResponseBodyFlushing(w http.ResponseWriter, body io.Reader) (int64, error) {
	bufPtr := pool.GetBuffer(pool.SizeSmall)
	defer pool.PutBuffer(bufPtr)

	buf := (*bufPtr)[:pool.SizeSmall]
	var written int64

	for {
		nr, er := body.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				flushResponse(w)
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}
```

- [ ] **Step 4: Branch `Handler.ServeHTTP` for SSE responses**

In `internal/server/proxy/handler.go`, replace the response-body setup after the `HEAD`, `204`, and `304` block with this code:

```go
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
```

The resulting section should no longer have a second `context.AfterFunc` below the pooled buffer allocation.

- [ ] **Step 5: Format and run the proxy tests**

Run:

```bash
gofmt -w internal/server/proxy/handler.go internal/server/proxy/streaming.go internal/server/proxy/handler_sse_test.go
go test ./internal/server/proxy -run 'TestHandler(FlushesEventStreamResponse|PreservesContentLengthForOrdinaryResponse)' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/server/proxy/handler.go internal/server/proxy/streaming.go internal/server/proxy/handler_sse_test.go
git commit -m "fix: flush SSE responses through HTTP tunnels"
```

## Task 3: Client-Side SSE Write Deadline Handling

**Files:**
- Create: `internal/client/tcp/pool_handler_test.go`
- Modify: `internal/client/tcp/pool_handler.go`

- [ ] **Step 1: Write the failing client regression test**

Create `internal/client/tcp/pool_handler_test.go`:

```go
package tcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"drip/internal/shared/protocol"
	"drip/internal/shared/stats"

	"go.uber.org/zap"
)

type recordingConn struct {
	net.Conn
	mu            sync.Mutex
	writeDeadline time.Time
}

func (c *recordingConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *recordingConn) lastWriteDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadline
}

func TestHandleHTTPStreamForwardsEventStreamWithoutShortWriteDeadline(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			t.Errorf("path = %q, want /events", r.URL.Path)
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Errorf("backend ResponseWriter does not support flush")
			return
		}

		_, _ = fmt.Fprint(w, "data: first\n\n")
		flusher.Flush()
		<-release
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		backend.Close()
	}()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend URL: %v", err)
	}
	localHost, localPortText, err := net.SplitHostPort(backendURL.Host)
	if err != nil {
		t.Fatalf("split backend host: %v", err)
	}
	localPort, err := strconv.Atoi(localPortText)
	if err != nil {
		t.Fatalf("parse backend port: %v", err)
	}

	poolClient := &PoolClient{
		tunnelType: protocol.TunnelTypeHTTP,
		localHost:  localHost,
		localPort:  localPort,
		httpClient: newLocalHTTPClient(protocol.TunnelTypeHTTP, false),
		ctx:        context.Background(),
		stats:      stats.NewTrafficStats(),
		logger:     zap.NewNop(),
	}

	serverSide, rawClientSide := net.Pipe()
	defer serverSide.Close()
	clientSide := &recordingConn{Conn: rawClientSide}

	done := make(chan struct{})
	go func() {
		defer close(done)
		poolClient.handleHTTPStream(clientSide)
	}()

	req, err := http.NewRequest(http.MethodGet, "http://demo.tunnels.test/events", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if err := req.Write(serverSide); err != nil {
		t.Fatalf("write request to stream: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(serverSide), req)
	if err != nil {
		t.Fatalf("read response from stream: %v", err)
	}
	defer resp.Body.Close()

	got := readClientBodyWithin(t, resp.Body, len("data: first\n\n"), time.Second)
	if got != "data: first\n\n" {
		t.Fatalf("body prefix = %q, want first SSE event", got)
	}

	if deadline := clientSide.lastWriteDeadline(); !deadline.IsZero() {
		t.Fatalf("SSE stream write deadline = %v, want zero", deadline)
	}

	releaseOnce.Do(func() { close(release) })
	_ = serverSide.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("handleHTTPStream did not return after stream close")
	}
}

func readClientBodyWithin(t *testing.T, body io.Reader, size int, timeout time.Duration) string {
	t.Helper()

	type readResult struct {
		data []byte
		err  error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		buf := make([]byte, size)
		_, err := io.ReadFull(body, buf)
		resultCh <- readResult{data: buf, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("read response body: %v", result.err)
		}
		return string(result.data)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for response body")
	}

	return ""
}
```

- [ ] **Step 2: Run the client test to verify it fails**

Run:

```bash
go test ./internal/client/tcp -run TestHandleHTTPStreamForwardsEventStreamWithoutShortWriteDeadline -count=1
```

Expected: FAIL with `SSE stream write deadline = ... want zero`.

- [ ] **Step 3: Modify `handleHTTPStream` to avoid SSE short write deadlines**

In `internal/client/tcp/pool_handler.go`, after the local backend response is received, compute whether the response is SSE:

```go
	isSSE := httputil.IsEventStream(resp.Header)

	_ = stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if err := writeResponseHeader(cc, resp); err != nil {
		return
	}
	if isSSE {
		_ = stream.SetWriteDeadline(time.Time{})
	}
```

Then replace the body copy loop with:

```go
	buf := make([]byte, 32*1024)
	for {
		nr, er := resp.Body.Read(buf)
		if nr > 0 {
			if !isSSE {
				_ = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
			}
			nw, ew := cc.Write(buf[:nr])
			if isSSE {
				_ = stream.SetWriteDeadline(time.Time{})
			}
			if ew != nil || nr != nw {
				break
			}
		}
		if er != nil {
			break
		}
	}
```

This keeps the existing write deadlines for ordinary responses while ensuring an SSE stream is not left with a short write deadline between events.

- [ ] **Step 4: Format and run the client test**

Run:

```bash
gofmt -w internal/client/tcp/pool_handler.go internal/client/tcp/pool_handler_test.go
go test ./internal/client/tcp -run TestHandleHTTPStreamForwardsEventStreamWithoutShortWriteDeadline -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/client/tcp/pool_handler.go internal/client/tcp/pool_handler_test.go
git commit -m "fix: keep SSE tunnel streams open"
```

## Task 4: Full Verification

**Files:**
- Verify: `internal/shared/httputil/streaming.go`
- Verify: `internal/server/proxy/handler.go`
- Verify: `internal/server/proxy/streaming.go`
- Verify: `internal/client/tcp/pool_handler.go`

- [ ] **Step 1: Run focused tests**

Run:

```bash
go test ./internal/shared/httputil ./internal/server/proxy ./internal/client/tcp -count=1
```

Expected: PASS.

- [ ] **Step 2: Run the full Go test suite**

Run:

```bash
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 3: Inspect the final diff**

Run:

```bash
git diff --stat HEAD
git diff HEAD -- internal/shared/httputil internal/server/proxy internal/client/tcp
```

Expected: only the SSE detection, server flushing branch, client deadline handling, and related tests are present.

- [ ] **Step 4: Final commit if verification changed files**

If formatting or verification edits changed files after Task 3, commit them:

```bash
git add internal/shared/httputil internal/server/proxy internal/client/tcp
git commit -m "test: verify SSE tunnel streaming"
```

If `git status --short` is clean, skip this commit.

## Self-Review

- Spec coverage: Task 1 covers SSE media type detection; Task 2 covers immediate public response flushing, body chunk flushing, and `Content-Length` removal for SSE; Task 3 covers client-side long-lived stream deadline behavior; Task 4 covers targeted and full verification.
- Scope check: the plan only changes HTTP/HTTPS tunnel code paths and shared HTTP header detection. TCP tunnel behavior, yamux sessions, auth, IP controls, and WebSocket upgrade handling stay outside the change set.
- Type consistency: `httputil.IsEventStream(http.Header)` is defined in Task 1 and reused from `internal/server/proxy` and `internal/client/tcp`; `copyResponseBodyFlushing(http.ResponseWriter, io.Reader)` is package-local to `internal/server/proxy`.
