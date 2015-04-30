package curator

import (
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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

type ConnectionStateTestSuite struct {
	suite.Suite

	ensembleProvider  *mockEnsembleProvider
	zookeeperDialer   *mockZookeeperDialer
	conn              *mockConn
	tracer            *mockTracerDriver
	sessionTimeout    time.Duration
	connectionTimeout time.Duration
	canBeReadOnly     bool
	events            chan zk.Event
	watcher           Watcher
	sessionEvents     []*zk.Event
	state             *connectionState
	connStrTimes      int
	dialTimes         int
	connCloseTimes    int
}

func TestConnectionState(t *testing.T) {
	suite.Run(t, &ConnectionStateTestSuite{
		sessionTimeout:    15 * time.Second,
		connectionTimeout: 5 * time.Second,
		connStrTimes:      1,
		dialTimes:         1,
		connCloseTimes:    1,
	})
}

func (s *ConnectionStateTestSuite) SetupTest() {
	s.ensembleProvider = &mockEnsembleProvider{log: s.T().Logf}
	s.zookeeperDialer = &mockZookeeperDialer{log: s.T().Logf}
	s.conn = &mockConn{log: s.T().Logf}
	s.tracer = &mockTracerDriver{log: s.T().Logf}
	s.events = make(chan zk.Event)

	s.watcher = NewWatcher(func(event *zk.Event) {
		s.sessionEvents = append(s.sessionEvents, event)
	})

	// create connection
	s.state = newConnectionState(s.zookeeperDialer, s.ensembleProvider, s.sessionTimeout, s.connectionTimeout, s.watcher, s.tracer, s.canBeReadOnly)

	assert.NotNil(s.T(), s.state)
	assert.False(s.T(), s.state.Connected())
}

func (s *ConnectionStateTestSuite) Start() {
	// start connection
	s.ensembleProvider.On("Start").Return(nil).Once()
	s.ensembleProvider.On("ConnectionString").Return("connStr").Times(s.connStrTimes)
	s.zookeeperDialer.On("Dial", "connStr", s.sessionTimeout, s.canBeReadOnly).Return(s.conn, s.events, nil).Times(s.dialTimes)
	s.conn.On("Close").Return().Times(s.connCloseTimes)

	assert.NoError(s.T(), s.state.Start())
	assert.False(s.T(), s.state.Connected())
}

func (s *ConnectionStateTestSuite) Close() {
	// close connection
	s.ensembleProvider.On("Close").Return(nil).Once()

	assert.NoError(s.T(), s.state.Close())
}

func (s *ConnectionStateTestSuite) TearDownTest() {
	s.ensembleProvider.AssertExpectations(s.T())
	s.zookeeperDialer.AssertExpectations(s.T())
	s.conn.AssertExpectations(s.T())
	s.tracer.AssertExpectations(s.T())

	s.sessionEvents = nil

	close(s.events)
}

func (s *ConnectionStateTestSuite) TestConnectionTimeout() {
	s.connStrTimes = 2

	s.Start()
	defer s.Close()

	instanceIndex := s.state.InstanceIndex()

	// force to connect timeout
	s.state.connectionStart = time.Now().Add(-s.connectionTimeout * 2)

	s.tracer.On("AddCount", "connections-timed-out", 1).Return().Once()

	conn, err := s.state.Conn()

	assert.Nil(s.T(), conn)
	assert.Equal(s.T(), ErrConnectionLoss, err)
	assert.Equal(s.T(), instanceIndex, s.state.InstanceIndex())
}

func (s *ConnectionStateTestSuite) TestSessionTimeout() {
	s.connStrTimes = 3
	s.dialTimes = 2
	s.connCloseTimes = 2

	s.Start()
	defer s.Close()

	instanceIndex := s.state.InstanceIndex()

	// force to session timeout
	s.state.connectionStart = time.Now().Add(-s.sessionTimeout * 2)

	s.tracer.On("AddCount", "session-timed-out", 1).Return().Once()

	conn, err := s.state.Conn()

	assert.NotNil(s.T(), conn)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), instanceIndex+1, s.state.InstanceIndex())
}

func (s *ConnectionStateTestSuite) TestBackgroundException() {
	s.tracer.On("AddCount", "background-exceptions", 1).Return().Times(2)

	// deque from empty queue
	assert.NoError(s.T(), s.state.dequeBackgroundException())

	// enque and deque
	s.state.queueBackgroundException(ErrConnectionLoss)

	assert.Equal(s.T(), ErrConnectionLoss, s.state.dequeBackgroundException())
	assert.NoError(s.T(), s.state.dequeBackgroundException())

	// enque too many errors
	s.tracer.On("AddCount", "connection-drop-background-error", 1).Return().Once()

	s.state.queueBackgroundException(zk.ErrAPIError)

	for i := 0; i < MAX_BACKGROUND_ERRORS; i++ {
		s.state.queueBackgroundException(ErrConnectionLoss)
	}

	assert.Equal(s.T(), ErrConnectionLoss, s.state.dequeBackgroundException())
}

func (s *ConnectionStateTestSuite) TestConnected() {
	s.connStrTimes = 2

	s.Start()
	defer s.Close()

	instanceIndex := s.state.InstanceIndex()

	// get the connection
	conn, err := s.state.Conn()

	assert.NotNil(s.T(), conn)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), instanceIndex, s.state.InstanceIndex())

	// receive a session event
	s.tracer.On("AddTime", "connection-state-parent-process", mock.AnythingOfType("Duration")).Return().Once()

	s.events <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateHasSession,
	}

	time.Sleep(100 * time.Microsecond)

	assert.True(s.T(), s.state.Connected())
}

func (s *ConnectionStateTestSuite) TestNewConnectionString() {

}

func (s *ConnectionStateTestSuite) TestExpiredSession() {
	s.connStrTimes = 3
	s.dialTimes = 2
	s.connCloseTimes = 2

	s.Start()
	defer s.Close()

	instanceIndex := s.state.InstanceIndex()

	// get the connection
	conn, err := s.state.Conn()

	assert.NotNil(s.T(), conn)
	assert.NoError(s.T(), err)

	// receive StateHasSession event
	s.tracer.On("AddTime", "connection-state-parent-process", mock.AnythingOfType("Duration")).Return().Twice()

	s.events <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateHasSession,
	}

	time.Sleep(100 * time.Microsecond)

	assert.True(s.T(), s.state.Connected())

	// receive StateExpired event
	s.tracer.On("AddCount", "session-expired", 1).Return().Once()

	s.events <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateExpired,
	}

	time.Sleep(100 * time.Microsecond)

	assert.Equal(s.T(), instanceIndex+1, s.state.InstanceIndex())
	assert.False(s.T(), s.state.Connected())
}

func (s *ConnectionStateTestSuite) TestParentWatcher() {

}
