package tcp

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/hashicorp/yamux"

	"drip/internal/shared/mux"
	"drip/internal/shared/protocol"
)

var dataConnCounter atomic.Uint64

// sessionHandle wraps a yamux session with metadata.
type sessionHandle struct {
	id         string
	conn       net.Conn
	session    *yamux.Session
	active     atomic.Int64
	lastActive atomic.Int64 // unix nanos
	closed     atomic.Bool
}

func (h *sessionHandle) touch() {
	h.lastActive.Store(time.Now().UnixNano())
}

func (h *sessionHandle) lastActiveTime() time.Time {
	n := h.lastActive.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// warmupSessions pre-creates initial sessions in parallel to eliminate cold-start latency.
func (c *PoolClient) warmupSessions() {
	if c.IsClosed() || c.tunnelID == "" {
		return
	}

	c.mu.RLock()
	desired := c.desiredTotal
	c.mu.RUnlock()

	current := c.sessionCount()
	toCreate := desired - current
	if toCreate <= 0 {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < toCreate; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.addDataSession()
		}()
	}
	wg.Wait()

	// Brief wait for server to register all sessions
	time.Sleep(100 * time.Millisecond)
}

// addDataSession creates a new data session.
func (c *PoolClient) addDataSession() error {
	if err := c.reserveSessionSlot(); err != nil {
		return err
	}
	slotReserved := true
	defer func() {
		if slotReserved {
			c.releaseSessionSlot()
		}
	}()

	if c.tunnelID == "" {
		return fmt.Errorf("server does not support data connections")
	}

	conn, err := c.dialer.Dial()
	if err != nil {
		return err
	}

	if c.closedDuringHandshake(conn) {
		return net.ErrClosed
	}

	connID := fmt.Sprintf("data-%d", dataConnCounter.Add(1))

	req := protocol.DataConnectRequest{
		TunnelID:     c.tunnelID,
		Token:        c.token,
		ConnectionID: connID,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to marshal data connect request: %w", err)
	}

	if err := protocol.WriteFrame(conn, protocol.NewFrame(protocol.FrameTypeDataConnect, payload)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to send data connect: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	ack, err := protocol.ReadFrame(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to read data connect ack: %w", err)
	}
	defer ack.Release()
	_ = conn.SetReadDeadline(time.Time{})

	if c.closedDuringHandshake(conn) {
		return net.ErrClosed
	}

	if ack.Type == protocol.FrameTypeError {
		var errMsg protocol.ErrorMessage
		if e := json.Unmarshal(ack.Payload, &errMsg); e == nil {
			_ = conn.Close()
			return fmt.Errorf("data connect error: %s - %s", errMsg.Code, errMsg.Message)
		}
		_ = conn.Close()
		return fmt.Errorf("data connect error")
	}
	if ack.Type != protocol.FrameTypeDataConnectAck {
		_ = conn.Close()
		return fmt.Errorf("unexpected data connect ack frame: %s", ack.Type)
	}

	var resp protocol.DataConnectResponse
	if err := json.Unmarshal(ack.Payload, &resp); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to parse data connect response: %w", err)
	}
	if !resp.Accepted {
		_ = conn.Close()
		return fmt.Errorf("data connection rejected: %s", resp.Message)
	}

	yamuxCfg := mux.NewClientConfig()

	session, err := yamux.Server(conn, yamuxCfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to init yamux session: %w", err)
	}

	h := &sessionHandle{
		id:      connID,
		conn:    conn,
		session: session,
	}
	h.touch()

	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = session.Close()
		_ = conn.Close()
		return net.ErrClosed
	}
	c.pendingSessions--
	slotReserved = false
	c.dataSessions[connID] = h
	c.mu.Unlock()

	c.wg.Add(1)
	go c.acceptLoop(h, false)

	c.wg.Add(1)
	go c.sessionWatcher(h, false)

	c.wg.Add(1)
	go c.pingLoop(h)

	return nil
}

func (c *PoolClient) reserveSessionSlot() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return net.ErrClosed
	}

	select {
	case <-c.stopCh:
		return net.ErrClosed
	default:
	}

	if c.sessionCountLocked() >= c.maxSessions {
		return fmt.Errorf("max sessions reached")
	}

	c.pendingSessions++
	return nil
}

func (c *PoolClient) releaseSessionSlot() {
	c.mu.Lock()
	if c.pendingSessions > 0 {
		c.pendingSessions--
	}
	c.mu.Unlock()
}

func (c *PoolClient) closedDuringHandshake(conn net.Conn) bool {
	if c.closed.Load() {
		_ = conn.Close()
		return true
	}
	select {
	case <-c.stopCh:
		_ = conn.Close()
		return true
	default:
		return false
	}
}

func (c *PoolClient) sessionCountLocked() int {
	count := len(c.dataSessions) + c.pendingSessions
	if c.primary != nil {
		count++
	}
	return count
}

// removeIdleSessions removes n idle sessions.
func (c *PoolClient) removeIdleSessions(n int) {
	if n <= 0 {
		return
	}

	type candidate struct {
		id         string
		active     int64
		lastActive time.Time
	}

	c.mu.RLock()
	candidates := make([]candidate, 0, len(c.dataSessions))
	for id, h := range c.dataSessions {
		candidates = append(candidates, candidate{
			id:         id,
			active:     h.active.Load(),
			lastActive: h.lastActiveTime(),
		})
	}
	c.mu.RUnlock()

	removed := 0
	for removed < n {
		var best candidate
		found := false
		for _, cand := range candidates {
			if cand.active != 0 {
				continue
			}
			if !found || cand.lastActive.Before(best.lastActive) {
				best = cand
				found = true
			}
		}
		if !found {
			return
		}
		if c.removeDataSession(best.id) {
			removed++
		}
		for i := range candidates {
			if candidates[i].id == best.id {
				candidates[i].active = 1
				break
			}
		}
	}
}

// removeDataSession removes a data session by ID.
func (c *PoolClient) removeDataSession(id string) bool {
	var h *sessionHandle

	c.mu.Lock()
	h = c.dataSessions[id]
	delete(c.dataSessions, id)
	c.mu.Unlock()

	if h == nil {
		return false
	}

	if !h.closed.CompareAndSwap(false, true) {
		return false
	}

	if h.session != nil {
		_ = h.session.Close()
	}
	if h.conn != nil {
		_ = h.conn.Close()
	}

	return true
}

// sessionCount returns the total number of active sessions.
func (c *PoolClient) sessionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionCountLocked()
}

// SessionStats holds per-session statistics.
type SessionStats struct {
	ID           string
	IsPrimary    bool
	ActiveCount  int64
	LastActiveAt time.Time
}

// GetSessionStats returns statistics for all sessions.
func (c *PoolClient) GetSessionStats() []SessionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make([]SessionStats, 0, len(c.dataSessions)+1)

	if c.primary != nil {
		stats = append(stats, SessionStats{
			ID:           c.primary.id,
			IsPrimary:    true,
			ActiveCount:  c.primary.active.Load(),
			LastActiveAt: c.primary.lastActiveTime(),
		})
	}

	for _, h := range c.dataSessions {
		stats = append(stats, SessionStats{
			ID:           h.id,
			IsPrimary:    false,
			ActiveCount:  h.active.Load(),
			LastActiveAt: h.lastActiveTime(),
		})
	}

	return stats
}
