package curator

import (
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestHandleHolder(t *testing.T) {
	ensembleProvider := &mockEnsembleProvider{log: t.Logf}
	zookeeperDialer := &mockZookeeperDialer{log: t.Logf}
	zookeeperConnection := &mockConn{log: t.Logf}
	events := make(chan zk.Event)
	watcher := NewWatcher(func(event *zk.Event) {})

	// create new connection holder
	h := &handleHolder{
		zookeeperDialer:  zookeeperDialer,
		ensembleProvider: ensembleProvider,
		watcher:          watcher,
		sessionTimeout:   15 * time.Second,
	}

	assert.Equal(t, "", h.getConnectionString())
	assert.False(t, h.hasNewConnectionString())
	conn, err := h.getZookeeperConnection()
	assert.Nil(t, conn)
	assert.NoError(t, err)

	// close and reset helper
	assert.NoError(t, h.closeAndReset())

	assert.NotNil(t, h.helper)
	assert.Equal(t, "", h.getConnectionString())
	assert.IsType(t, (*zookeeperFactory)(nil), h.helper)

	// get and create connection
	ensembleProvider.On("ConnectionString").Return("connStr").Once()
	zookeeperDialer.On("Dial", "connStr", h.sessionTimeout, h.canBeReadOnly).Return(zookeeperConnection, events, nil).Once()

	conn, err = h.getZookeeperConnection()

	assert.NotNil(t, conn)
	assert.NoError(t, err)
	assert.Equal(t, "connStr", h.getConnectionString())
	assert.NotNil(t, h.helper)
	assert.IsType(t, (*zookeeperCache)(nil), h.helper)

	// close connection
	zookeeperConnection.On("Close").Return(nil).Once()

	assert.NoError(t, h.closeAndClear())

	close(events)

	ensembleProvider.AssertExpectations(t)
	zookeeperDialer.AssertExpectations(t)
	zookeeperConnection.AssertExpectations(t)
}

func TestZookeeperConnectionState(t *testing.T) {
	ensembleProvider := &mockEnsembleProvider{log: t.Logf}
	zookeeperDialer := &mockZookeeperDialer{log: t.Logf}
	conn := &mockConn{log: t.Logf}
	tracer := &mockTracerDriver{log: t.Logf}

	sessionTimeout := 15 * time.Second
	connectionTimeout := 5 * time.Second
	canBeReadOnly := false
	events := make(chan zk.Event)

	var sessionEvents []*zk.Event

	watcher := NewWatcher(func(event *zk.Event) {
		sessionEvents = append(sessionEvents, event)
	})

	// create connection
	s := newConnectionState(zookeeperDialer, ensembleProvider, sessionTimeout, connectionTimeout, watcher, tracer, canBeReadOnly)

	assert.NotNil(t, s)
	assert.False(t, s.Connected())

	// start connection
	ensembleProvider.On("Start").Return(nil).Once()
	ensembleProvider.On("ConnectionString").Return("connStr").Once()
	zookeeperDialer.On("Dial", "connStr", sessionTimeout, canBeReadOnly).Return(conn, events, nil).Once()

	assert.NoError(t, s.Start())
	assert.False(t, s.Connected())

	// close connection
	ensembleProvider.On("Close").Return(nil).Once()
	conn.On("Close").Return(nil).Once()

	assert.NoError(t, s.Close())

	ensembleProvider.AssertExpectations(t)
	zookeeperDialer.AssertExpectations(t)
	conn.AssertExpectations(t)
	tracer.AssertExpectations(t)
}
