package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DeleteBuilderTestSuite struct {
	suite.Suite

	conn    *mockConn
	dialer  *mockZookeeperDialer
	builder *CuratorFrameworkBuilder
	events  chan zk.Event
	wg      sync.WaitGroup
}

func TestDeleteBuilder(t *testing.T) {
	suite.Run(t, new(DeleteBuilderTestSuite))
}

func (s *DeleteBuilderTestSuite) SetupTest() {
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

func (s *DeleteBuilderTestSuite) TearDownTest() {
	close(s.events)

	s.conn.AssertExpectations(s.T())
	s.dialer.AssertExpectations(s.T())
}

func (s *DeleteBuilderTestSuite) TestDelete() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Delete", "/node", -1).Return(nil).Once()

	assert.NoError(s.T(), client.Delete().ForPath("/node"))

	assert.NoError(s.T(), client.Close())
}

func (s *DeleteBuilderTestSuite) TestDeleteWithVersion() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Delete", "/node", 1).Return(zk.ErrBadVersion).Once()

	assert.EqualError(s.T(), client.Delete().WithVersion(1).ForPath("/node"), zk.ErrBadVersion.Error())

	assert.NoError(s.T(), client.Close())
}
