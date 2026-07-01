package tcp

import (
	"testing"
	"time"

	"drip/internal/shared/protocol"

	"go.uber.org/zap"
)

func TestCleanupStaleGroupsStopsHeartbeat(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	manager := &ConnectionGroupManager{
		groups:       make(map[string]*ConnectionGroup),
		logger:       logger,
		staleTimeout: time.Millisecond,
	}

	group := NewConnectionGroup("tunnel-1", "app", "token", nil, protocol.TunnelTypeTCP, logger)
	group.LastActivity = time.Now().Add(-time.Hour)
	manager.groups[group.TunnelID] = group
	group.StartHeartbeat(time.Millisecond, 10*time.Millisecond)

	manager.cleanupStaleGroups()

	select {
	case <-group.stopCh:
	case <-time.After(time.Second):
		t.Fatal("stale group stopCh was not closed")
	}

	if _, ok := manager.groups[group.TunnelID]; ok {
		t.Fatal("stale group was not removed from manager")
	}
}

func TestCleanupStaleGroupsStopsHeartbeatWithPrimaryConn(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	manager := &ConnectionGroupManager{
		groups:       make(map[string]*ConnectionGroup),
		logger:       logger,
		staleTimeout: time.Millisecond,
	}

	primary := NewConnection(ConnectionConfig{
		Logger: logger,
	})

	group := NewConnectionGroup("tunnel-2", "app", "token", primary, protocol.TunnelTypeTCP, logger)
	group.LastActivity = time.Now().Add(-time.Hour)
	manager.groups[group.TunnelID] = group
	group.StartHeartbeat(time.Millisecond, 10*time.Millisecond)

	manager.cleanupStaleGroups()

	select {
	case <-group.stopCh:
	case <-time.After(time.Second):
		t.Fatal("stale group stopCh was not closed after primary cleanup")
	}

	if _, ok := manager.groups[group.TunnelID]; ok {
		t.Fatal("stale group was not removed from manager after primary cleanup")
	}
}
