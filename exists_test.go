package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CheckExistsBuilderTestSuite struct {
	suite.Suite

	conn    *mockConn
	dialer  *mockZookeeperDialer
	builder *CuratorFrameworkBuilder
	events  chan zk.Event
}

func TestCheckExistsBuilder(t *testing.T) {
	suite.Run(t, new(CheckExistsBuilderTestSuite))
}

func (s *CheckExistsBuilderTestSuite) SetupTest() {
	s.conn = &mockConn{log: s.T().Logf}
	s.dialer = &mockZookeeperDialer{log: s.T().Logf}
	s.builder = &CuratorFrameworkBuilder{
		ZookeeperDialer:  s.dialer,
		EnsembleProvider: &fixedEnsembleProvider{"connectString"},
		RetryPolicy:      NewRetryOneTime(0),
	}
	s.events = make(chan zk.Event)

	s.conn.On("Close").Return().Once()
	s.dialer.On("Dial", s.builder.EnsembleProvider.ConnectionString(), DEFAULT_CONNECTION_TIMEOUT, s.builder.CanBeReadOnly).Return(s.conn, s.events, nil).Once()
}

func (s *CheckExistsBuilderTestSuite) TearDownTest() {
	close(s.events)

	s.conn.AssertExpectations(s.T())
	s.dialer.AssertExpectations(s.T())
}

func (s *CheckExistsBuilderTestSuite) TestCheckExists() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Exists", "/node").Return(true, &zk.Stat{}, nil).Once()

	stat, err := client.CheckExists().ForPath("/node")

	assert.NotNil(s.T(), stat)
	assert.NoError(s.T(), err)

	// not exists
	s.conn.On("Exists", "/node").Return(false, nil, nil).Once()

	stat, err = client.CheckExists().ForPath("/node")

	assert.Nil(s.T(), stat)
	assert.EqualError(s.T(), err, zk.ErrNoNode.Error())

	// api failed
	s.conn.On("Exists", "/node").Return(false, nil, zk.ErrAPIError).Once()

	stat, err = client.CheckExists().ForPath("/node")

	assert.Nil(s.T(), stat)
	assert.EqualError(s.T(), err, zk.ErrAPIError.Error())

	assert.NoError(s.T(), client.Close())
}

func (s *CheckExistsBuilderTestSuite) TestNamespace() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Exists", "/parent").Return(true, nil, nil).Once()
	s.conn.On("Exists", "/parent/child").Return(false, nil, nil).Once()

	stat, err := client.CheckExists().ForPath("/child")

	assert.Nil(s.T(), stat)
	assert.EqualError(s.T(), err, zk.ErrNoNode.Error())

	assert.NoError(s.T(), client.Close())
}

func (s *CheckExistsBuilderTestSuite) TestBackground() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	ctxt := "context"

	s.conn.On("Exists", "/parent").Return(true, nil, nil).Once()
	s.conn.On("Exists", "/parent/child").Return(true, &zk.Stat{}, nil).Once()

	var wg sync.WaitGroup

	wg.Add(1)

	stat, err := client.CheckExists().InBackgroundWithCallbackAndContext(
		func(client CuratorFramework, event CuratorEvent) error {
			defer wg.Done()

			assert.Equal(s.T(), EXISTS, event.Type())
			assert.Equal(s.T(), "/child", event.Path())
			assert.NotNil(s.T(), event.Stat())
			assert.NoError(s.T(), event.Err())
			assert.Equal(s.T(), "child", event.Name())
			assert.Equal(s.T(), ctxt, event.Context())

			return nil
		}, ctxt).ForPath("/child")

	assert.Nil(s.T(), stat)
	assert.NoError(s.T(), err)

	wg.Wait()

	assert.NoError(s.T(), client.Close())
}

func (s *CheckExistsBuilderTestSuite) TestWatcher() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	events := make(chan zk.Event)

	defer close(events)

	s.conn.On("ExistsW", "/node").Return(true, &zk.Stat{}, events, nil).Once()

	var wg sync.WaitGroup

	wg.Add(1)

	stat, err := client.CheckExists().UsingWatcher(NewWatcher(func(event *zk.Event) {
		defer wg.Done()

		assert.NotNil(s.T(), event)
		assert.Equal(s.T(), zk.EventNodeDeleted, event.Type)
		assert.Equal(s.T(), "/node", event.Path)

	})).ForPath("/node")

	assert.NotNil(s.T(), stat)
	assert.NoError(s.T(), err)

	events <- zk.Event{
		Type: zk.EventNodeDeleted,
		Path: "/node",
	}

	wg.Wait()

	assert.NoError(s.T(), client.Close())
}
