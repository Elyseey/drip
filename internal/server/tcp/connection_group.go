package tcp

import (
	"container/heap"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"

	"drip/internal/shared/constants"
	"drip/internal/shared/protocol"

	"go.uber.org/zap"
)

// sessionEntry represents a session with its current stream count for heap operations
type sessionEntry struct {
	id      string
	session *yamux.Session
	streams int
	heapIdx int // index in the heap, managed by heap.Interface
}

// sessionHeap implements heap.Interface for O(log n) session selection
type sessionHeap []*sessionEntry

func (h sessionHeap) Len() int { return len(h) }

func (h sessionHeap) Less(i, j int) bool {
	// Min-heap: session with fewer streams has higher priority
	return h[i].streams < h[j].streams
}

func (h sessionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *sessionHeap) Push(x interface{}) {
	entry := x.(*sessionEntry)
	entry.heapIdx = len(*h)
	*h = append(*h, entry)
}

func (h *sessionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil // avoid memory leak
	entry.heapIdx = -1
	*h = old[0 : n-1]
	return entry
}

// sessionHeapPool reuses heap slices to reduce allocations
var sessionHeapPool = sync.Pool{
	New: func() interface{} {
		h := make(sessionHeap, 0, 16)
		return &h
	},
}

func putSessionHeap(h *sessionHeap) {
	if h == nil {
		return
	}
	*h = (*h)[:0]
	sessionHeapPool.Put(h)
}

type ConnectionGroup struct {
	TunnelID     string
	Subdomain    string
	Token        string
	PrimaryConn  *Connection
	Sessions     map[string]*yamux.Session
	TunnelType   protocol.TunnelType
	RegisteredAt time.Time
	LastActivity time.Time
	mu           sync.RWMutex
	stopCh       chan struct{}
	logger       *zap.Logger

	heartbeatStarted bool
}

func NewConnectionGroup(tunnelID, subdomain, token string, primaryConn *Connection, tunnelType protocol.TunnelType, logger *zap.Logger) *ConnectionGroup {
	return &ConnectionGroup{
		TunnelID:     tunnelID,
		Subdomain:    subdomain,
		Token:        token,
		PrimaryConn:  primaryConn,
		Sessions:     make(map[string]*yamux.Session),
		TunnelType:   tunnelType,
		RegisteredAt: time.Now(),
		LastActivity: time.Now(),
		stopCh:       make(chan struct{}),
		logger:       logger.With(zap.String("tunnel_id", tunnelID)),
	}
}

// StartHeartbeat starts a goroutine that periodically pings all sessions
// and removes dead ones. The caller should ensure this is only called once.
func (g *ConnectionGroup) StartHeartbeat(interval, timeout time.Duration) {
	go g.heartbeatLoop(interval, timeout)
}

func (g *ConnectionGroup) heartbeatLoop(interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	const maxConsecutiveFailures = 3
	failureCount := make(map[string]int)
	pingInFlight := make(map[string]bool)
	// pendingDone tracks Ping results that outlived their wait timeout so we
	// never start overlapping pings (and leak goroutines) on a wedged session.
	pendingDone := make(map[string]<-chan error)

	type sessionSnapshot struct {
		id      string
		session *yamux.Session
	}
	sessions := make([]sessionSnapshot, 0, 16)

	handlePingResult := func(id string, err error) {
		if err != nil {
			failureCount[id]++
			g.logger.Debug("Session ping failed",
				zap.String("session_id", id),
				zap.Int("consecutive_failures", failureCount[id]),
				zap.Error(err),
			)

			if failureCount[id] >= maxConsecutiveFailures {
				if id == "primary" {
					g.logger.Warn("Primary session ping failed repeatedly, keeping session alive",
						zap.String("session_id", id),
						zap.Int("failures", failureCount[id]),
					)
					failureCount[id] = 0
				} else {
					g.logger.Warn("Session ping failed too many times, removing",
						zap.String("session_id", id),
						zap.Int("failures", failureCount[id]),
					)
					g.RemoveSession(id)
					delete(failureCount, id)
					delete(pingInFlight, id)
					delete(pendingDone, id)
				}
			}
			return
		}

		failureCount[id] = 0
		g.mu.Lock()
		g.LastActivity = time.Now()
		g.mu.Unlock()
	}

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
		}

		sessions = sessions[:0]
		g.mu.RLock()
		for id, s := range g.Sessions {
			sessions = append(sessions, sessionSnapshot{id: id, session: s})
		}
		g.mu.RUnlock()

		for _, snap := range sessions {
			if snap.session == nil || snap.session.IsClosed() {
				g.RemoveSession(snap.id)
				delete(failureCount, snap.id)
				delete(pingInFlight, snap.id)
				delete(pendingDone, snap.id)
				continue
			}

			if ch, ok := pendingDone[snap.id]; ok {
				select {
				case err := <-ch:
					delete(pendingDone, snap.id)
					delete(pingInFlight, snap.id)
					// Timeout already counted as a failure; only apply a late
					// success so we can clear the failure streak.
					if err == nil {
						handlePingResult(snap.id, nil)
					}
				default:
					// Previous ping still running; do not start another.
				}
				continue
			}

			if pingInFlight[snap.id] {
				continue
			}
			pingInFlight[snap.id] = true

			done := make(chan error, 1)
			go func(s *yamux.Session) {
				_, err := s.Ping()
				done <- err
			}(snap.session)

			var err error
			select {
			case err = <-done:
				delete(pingInFlight, snap.id)
				handlePingResult(snap.id, err)
			case <-time.After(timeout):
				// Keep pingInFlight set and wait for the real completion on a
				// later tick so overlapping Ping goroutines cannot accumulate.
				pendingDone[snap.id] = done
				handlePingResult(snap.id, fmt.Errorf("ping timeout"))
			case <-g.stopCh:
				delete(pingInFlight, snap.id)
				delete(pendingDone, snap.id)
				return
			}
		}

		g.mu.RLock()
		sessionCount := len(g.Sessions)
		g.mu.RUnlock()

		if sessionCount == 0 {
			g.logger.Info("All sessions closed, tunnel will be cleaned up")
		}
	}
}

func (g *ConnectionGroup) Close() {
	g.mu.Lock()

	select {
	case <-g.stopCh:
		g.mu.Unlock()
		return
	default:
		close(g.stopCh)
	}

	sessions := make([]*yamux.Session, 0, len(g.Sessions))
	for _, session := range g.Sessions {
		if session != nil {
			sessions = append(sessions, session)
		}
	}
	g.Sessions = make(map[string]*yamux.Session)

	g.mu.Unlock()

	for _, session := range sessions {
		_ = session.Close()
	}
}

func (g *ConnectionGroup) IsStale(timeout time.Duration) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return time.Since(g.LastActivity) > timeout
}

func (g *ConnectionGroup) AddSession(connID string, session *yamux.Session) {
	if connID == "" || session == nil {
		return
	}

	g.mu.Lock()
	if g.Sessions == nil {
		g.Sessions = make(map[string]*yamux.Session)
	}
	g.Sessions[connID] = session
	g.LastActivity = time.Now()

	// Start heartbeat on first session
	shouldStartHeartbeat := !g.heartbeatStarted
	if shouldStartHeartbeat {
		g.heartbeatStarted = true
	}
	g.mu.Unlock()

	if shouldStartHeartbeat {
		g.StartHeartbeat(constants.HeartbeatInterval, constants.HeartbeatTimeout)
	}
}

func (g *ConnectionGroup) RemoveSession(connID string) {
	if connID == "" {
		return
	}

	var session *yamux.Session

	g.mu.Lock()
	if g.Sessions != nil {
		session = g.Sessions[connID]
		delete(g.Sessions, connID)
	}
	g.mu.Unlock()

	if session != nil {
		_ = session.Close()
	}
}

func (g *ConnectionGroup) SessionCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.Sessions)
}

// OpenStream opens a new stream using a min-heap for O(log n) session selection.
func (g *ConnectionGroup) OpenStream() (net.Conn, error) {
	const (
		maxStreamsPerSession = 256
		maxRetries           = 3
		backoffBase          = 5 * time.Millisecond
	)

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-g.stopCh:
			return nil, net.ErrClosed
		default:
		}

		h := g.buildSessionHeap(false)
		if h.Len() == 0 {
			putSessionHeap(h)
			h = g.buildSessionHeap(true)
		}
		if h.Len() == 0 {
			putSessionHeap(h)
			return nil, net.ErrClosed
		}

		anyUnderCap := false
		for h.Len() > 0 {
			entry := heap.Pop(h).(*sessionEntry)
			session := entry.session

			if session == nil || session.IsClosed() {
				continue
			}

			currentStreams := session.NumStreams()
			if currentStreams >= maxStreamsPerSession {
				continue
			}
			anyUnderCap = true

			stream, err := session.Open()
			if err == nil {
				putSessionHeap(h)
				return stream, nil
			}
			lastErr = err

			if session.IsClosed() {
				g.deleteClosedSessions()
			}
		}

		putSessionHeap(h)

		if !anyUnderCap {
			lastErr = fmt.Errorf("all sessions are at stream capacity (%d)", maxStreamsPerSession)
		}

		if attempt < maxRetries-1 {
			select {
			case <-g.stopCh:
				return nil, net.ErrClosed
			case <-time.After(backoffBase * time.Duration(attempt+1)):
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("failed to open stream")
	}
	return nil, lastErr
}

// buildSessionHeap creates a min-heap of sessions ordered by stream count.
func (g *ConnectionGroup) buildSessionHeap(includePrimary bool) *sessionHeap {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.Sessions) == 0 {
		h := sessionHeapPool.Get().(*sessionHeap)
		return h
	}

	h := sessionHeapPool.Get().(*sessionHeap)
	*h = (*h)[:0]

	for id, session := range g.Sessions {
		if session == nil || session.IsClosed() {
			continue
		}
		if id == "primary" && !includePrimary {
			continue
		}

		*h = append(*h, &sessionEntry{
			id:      id,
			session: session,
			streams: session.NumStreams(),
		})
	}

	heap.Init(h)
	return h
}

func (g *ConnectionGroup) deleteClosedSessions() {
	g.mu.Lock()
	for id, session := range g.Sessions {
		if session == nil || session.IsClosed() {
			delete(g.Sessions, id)
		}
	}
	g.mu.Unlock()
}
