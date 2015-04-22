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

func (s *DeleteBuilderTestSuite) TestNamespace() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Exists", "/parent").Return(true, nil, nil).Once()
	s.conn.On("Delete", "/parent/child", -1).Return(nil).Once()

	assert.NoError(s.T(), client.Delete().ForPath("/child"))

	assert.NoError(s.T(), client.Close())
}

func (s *DeleteBuilderTestSuite) TestBackground() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	ctxt := "context"

	s.conn.On("Exists", "/parent").Return(true, nil, nil).Once()
	s.conn.On("Delete", "/parent/child", -1).Return(nil).Once()

	s.wg.Add(1)

	assert.NoError(s.T(), client.Delete().InBackgroundWithCallbackAndContext(
		func(client CuratorFramework, event CuratorEvent) error {
			defer s.wg.Done()

			assert.Equal(s.T(), DELETE, event.Type())
			assert.Equal(s.T(), "/child", event.Path())
			assert.NoError(s.T(), event.Err())
			assert.Equal(s.T(), "child", event.Name())
			assert.Equal(s.T(), ctxt, event.Context())

			return nil
		}, ctxt).ForPath("/child"))

	s.wg.Wait()

	assert.NoError(s.T(), client.Close())
}

func (s *DeleteBuilderTestSuite) TestDeletingChildren() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Delete", "/parent", -1).Return(zk.ErrNotEmpty).Once()
	s.conn.On("Children", "/parent").Return([]string{"child"}, nil, nil).Once()
	s.conn.On("Children", "/parent/child").Return([]string{}, nil, nil).Once()
	s.conn.On("Delete", "/parent/child", -1).Return(nil).Once()
	s.conn.On("Delete", "/parent", -1).Return(nil).Once()

	assert.NoError(s.T(), client.Delete().DeletingChildrenIfNeeded().ForPath("/parent"))

	assert.NoError(s.T(), client.Close())
}
