package curator

import (
	"errors"
	"fmt"
	"time"
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

const (
	MAX_BACKGROUND_EXCEPTIONS = 10
)

type ZookeeperConnectionState struct {
	zookeeperDialer   ZookeeperDialer
	ensembleProvider  EnsembleProvider
	sessionTimeout    time.Duration
	connectionTimeout time.Duration
	tracer            TracerDriver
	canReadOnly       bool
	authInfos         []AuthInfo
	parentWatchers    *Watchers
	conn              ZookeeperConnection
}

func newZookeeperConnectionState(zookeeperDialer ZookeeperDialer, ensembleProvider EnsembleProvider, sessionTimeout, connectionTimeout time.Duration,
	watcher Watcher, tracer TracerDriver, canReadOnly bool, authInfos []AuthInfo) *ZookeeperConnectionState {

	s := &ZookeeperConnectionState{
		zookeeperDialer:   zookeeperDialer,
		ensembleProvider:  ensembleProvider,
		sessionTimeout:    sessionTimeout,
		connectionTimeout: connectionTimeout,
		tracer:            tracer,
		canReadOnly:       canReadOnly,
		authInfos:         authInfos,
		parentWatchers:    NewWatchers(),
	}

	if zookeeperDialer == nil {
		s.zookeeperDialer = &DefaultZookeeperDialer{}
	}

	if watcher != nil {
		s.parentWatchers.Add(watcher)
	}

	return s
}

func (s *ZookeeperConnectionState) isConnected() bool {
	return s.conn != nil
}

func (s *ZookeeperConnectionState) Conn() (ZookeeperConnection, error) {
	if s.conn != nil {
		return s.conn, nil
	}

	if conn, events, err := s.zookeeperDialer.Dial(s.ensembleProvider.ConnectionString(), s.connectionTimeout, s.canReadOnly); err != nil {
		return nil, err
	} else {
		for _, authInfo := range s.authInfos {
			if err := conn.AddAuth(authInfo.Scheme, authInfo.Auth); err != nil {
				conn.Close()

				return nil, err
			}
		}

		s.conn = conn

		if events != nil {
			go s.parentWatchers.Watch(events)
		}

		return conn, nil
	}
}

func (s *ZookeeperConnectionState) Start() error {
	if err := s.ensembleProvider.Start(); err != nil {
		return err
	}

	return s.reset()
}

func (s *ZookeeperConnectionState) Close() error {
	err := CloseQuietly(s.ensembleProvider)

	s.conn.Close()
	s.conn = nil

	return err
}

func (s *ZookeeperConnectionState) reset() error {
	if s.isConnected() {
		CloseQuietly(s)
	}

	_, err := s.Conn()

	return err
}

func (s *ZookeeperConnectionState) addParentWatcher(watcher Watcher) Watcher {
	return s.parentWatchers.Add(watcher)
}

func (s *ZookeeperConnectionState) removeParentWatcher(watcher Watcher) Watcher {
	return s.parentWatchers.Remove(watcher)
}

type connectionStateManager struct {
	client                 CuratorFramework
	listeners              ConnectionStateListenable
	state                  State
	currentConnectionState ConnectionState
	events                 chan ConnectionState
	QueueSize              int
}

func newConnectionStateManager(client CuratorFramework) *connectionStateManager {
	return &connectionStateManager{
		client:    client,
		listeners: new(connectionStateListenerContainer),
		QueueSize: 25,
	}
}

func (m *connectionStateManager) Start() error {
	if !m.state.Change(LATENT, STARTED) {
		return fmt.Errorf("Cannot be started more than once")
	}

	m.events = make(chan ConnectionState, m.QueueSize)

	go m.processEvents()

	return nil
}

func (m *connectionStateManager) Close() error {
	if !m.state.Change(STARTED, STOPPED) {
		return nil
	}

	close(m.events)

	return nil
}

func (m *connectionStateManager) processEvents() {
	for {
		if newState, ok := <-m.events; !ok {
			return // queue closed
		} else {
			m.listeners.ForEach(func(listener interface{}) {
				listener.(ConnectionStateListener).StateChanged(m.client, newState)
			})
		}
	}
}

func (m *connectionStateManager) postState(state ConnectionState) {
	for {
		select {
		case m.events <- state:
			return
		default:
		}

		select {
		case <-m.events: // "ConnectionStateManager queue full - dropping events to make room"
		default:
		}
	}
}

func (m *connectionStateManager) BlockUntilConnected(maxWaitTime time.Duration) error {
	c := make(chan ConnectionState)

	listener := NewConnectionStateListener(func(client CuratorFramework, newState ConnectionState) {
		if newState.Connected() {
			c <- newState
		}
	})

	m.listeners.AddListener(listener)

	defer m.listeners.RemoveListener(listener)

	if maxWaitTime > 0 {
		timer := time.NewTimer(maxWaitTime)

		select {
		case <-c:
			return nil
		case <-timer.C:
			return errors.New("timeout")
		}
	} else {
		<-c

		return nil
	}
}
