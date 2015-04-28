package curator

import (
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestZookeeperConnectionState(t *testing.T) {
	ensembleProvider := &mockEnsembleProvider{log: t.Logf}
	zookeeperDialer := &mockZookeeperDialer{log: t.Logf}
	conn := &mockConn{log: t.Logf}
	tracer := &mockTracerDriver{log: t.Logf}

	sessionTimeout := 15 * time.Second
	connectionTimeout := 5 * time.Second
	canBeReadOnly := false
	events := make(chan zk.Event)

	watcher := NewWatcher(func(event *zk.Event) {

	})

	s := newConnectionState(zookeeperDialer, ensembleProvider, sessionTimeout, connectionTimeout, watcher, tracer, canBeReadOnly)

	assert.NotNil(t, s)
	assert.False(t, s.Connected())

	ensembleProvider.On("Start").Return(nil).Once()
	ensembleProvider.On("ConnectionString").Return("connStr").Once()
	zookeeperDialer.On("Dial", "connStr", sessionTimeout, canBeReadOnly).Return(conn, events, nil).Once()

	assert.NoError(t, s.Start())

	ensembleProvider.On("Close").Return(nil).Once()
	conn.On("Close").Return(nil).Once()

	assert.NoError(t, s.Close())

	ensembleProvider.AssertExpectations(t)
	zookeeperDialer.AssertExpectations(t)
	conn.AssertExpectations(t)
	tracer.AssertExpectations(t)
}
