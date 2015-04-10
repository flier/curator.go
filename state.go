package curator

import (
	"fmt"
)

type ConnectionState int32

const (
	UNKNOWN     ConnectionState = iota
	CONNECTED                   // Sent for the first successful connection to the server.
	SUSPENDED                   // There has been a loss of connection. Leaders, locks, etc.
	RECONNECTED                 // A suspended, lost, or read-only connection has been re-established
	LOST                        // The connection is confirmed to be lost. Close any locks, leaders, etc.
	READ_ONLY                   // The connection has gone into read-only mode.
)

func (s ConnectionState) Connected() bool {
	return s == CONNECTED || s == RECONNECTED || s == READ_ONLY
}

type ConnectionStateManager struct {
	client                 CuratorFramework
	listeners              ConnectionStateListenable
	state                  CuratorFrameworkState
	currentConnectionState ConnectionState
}

func NewConnectionStateManager(client CuratorFramework) *ConnectionStateManager {
	return &ConnectionStateManager{
		client:    client,
		listeners: NewConnectionStateListenerContainer(),
	}
}

func (m *ConnectionStateManager) Start() error {
	if !m.state.Change(LATENT, STARTED) {
		return fmt.Errorf("Cannot be started more than once")
	}

	return nil
}

func (m *ConnectionStateManager) Close() error {
	if !m.state.Change(STARTED, STOPPED) {
		return nil
	}

	return nil
}
