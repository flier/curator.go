package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateBuilderTestSuite struct {
	suite.Suite

	conn        *mockConn
	dialer      *mockZookeeperDialer
	compress    *mockCompressionProvider
	aclProvider *mockACLProvider
	builder     *CuratorFrameworkBuilder
	events      chan zk.Event
	wg          sync.WaitGroup
}

func TestCreateBuilder(t *testing.T) {
	suite.Run(t, new(CreateBuilderTestSuite))
}

func (s *CreateBuilderTestSuite) SetupTest() {
	s.conn = &mockConn{log: s.T().Logf}
	s.dialer = &mockZookeeperDialer{log: s.T().Logf}
	s.compress = &mockCompressionProvider{log: s.T().Logf}
	s.builder = &CuratorFrameworkBuilder{
		ZookeeperDialer:     s.dialer,
		EnsembleProvider:    &fixedEnsembleProvider{"connectString"},
		CompressionProvider: s.compress,
		RetryPolicy:         NewRetryOneTime(0),
		DefaultData:         []byte("default"),
	}
	s.events = make(chan zk.Event)

	s.conn.On("Close").Return().Once()
	s.dialer.On("Dial", s.builder.EnsembleProvider.ConnectionString(), DEFAULT_CONNECTION_TIMEOUT, s.builder.CanBeReadOnly).Return(s.conn, s.events, nil).Once()
}

func (s *CreateBuilderTestSuite) TearDownTest() {
	close(s.events)

	s.conn.AssertExpectations(s.T())
	s.dialer.AssertExpectations(s.T())
	s.compress.AssertExpectations(s.T())
}

func (s *CreateBuilderTestSuite) TestCreate() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	acls := zk.WorldACL(zk.PermAll)

	s.conn.On("Create", "/node", s.builder.DefaultData, int32(EPHEMERAL), acls).Return("/node", nil).Once()

	path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("/node")

	assert.Equal(s.T(), "/node", path)
	assert.NoError(s.T(), err)

	assert.NoError(s.T(), client.Close())
}

func (s *CreateBuilderTestSuite) TestNamespace() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	acls := zk.WorldACL(zk.PermAll)

	s.conn.On("Exists", "/parent").Return(false, nil, nil).Once()
	s.conn.On("Create", "/parent", []byte{}, int32(PERSISTENT), acls).Return("/parent", nil).Once()
	s.conn.On("Create", "/parent/child", s.builder.DefaultData, int32(EPHEMERAL), acls).Return("/parent/child", nil).Once()

	path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("/child")

	assert.Equal(s.T(), "/child", path)
	assert.NoError(s.T(), err)

	assert.NoError(s.T(), client.Close())
}

func (s *CreateBuilderTestSuite) TestBackground() {
	s.builder.Namespace = "parent"

	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	data := []byte("data")
	acls := zk.AuthACL(zk.PermRead)
	ctxt := "context"

	s.conn.On("Exists", "/parent").Return(true, nil, nil).Once()
	s.conn.On("Create", "/parent/child", data, int32(PERSISTENT), acls).Return("", zk.ErrAPIError).Once()

	s.wg.Add(1)

	path, err := client.Create().WithACL(acls...).InBackgroundWithCallbackAndContext(
		func(client CuratorFramework, event CuratorEvent) error {
			defer s.wg.Done()

			assert.Equal(s.T(), CREATE, event.Type())
			assert.Equal(s.T(), "/child", event.Path())
			assert.Equal(s.T(), data, event.Data())
			assert.Equal(s.T(), acls, event.ACLs())
			assert.EqualError(s.T(), event.Err(), zk.ErrAPIError.Error())
			assert.Equal(s.T(), ctxt, event.Context())

			return nil
		}, ctxt).ForPathWithData("/child", data)

	s.wg.Wait()

	assert.Equal(s.T(), "/child", path)
	assert.NoError(s.T(), err)

	assert.NoError(s.T(), client.Close())
}

func (s *CreateBuilderTestSuite) TestCompression() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	data := []byte("data")
	compressedData := []byte("compressedData")
	acls := zk.WorldACL(zk.PermAll)

	s.compress.On("Compress", "/node", data).Return(compressedData, nil).Once()
	s.conn.On("Create", "/node", compressedData, int32(PERSISTENT), acls).Return("/node", nil).Once()

	path, err := client.Create().Compressed().WithACL(acls...).ForPathWithData("/node", data)

	assert.Equal(s.T(), "/node", path)
	assert.NoError(s.T(), err)

	assert.NoError(s.T(), client.Close())
}

func (s *CreateBuilderTestSuite) TestCreateParents() {
	client := s.builder.Build()

	assert.NoError(s.T(), client.Start())

	s.conn.On("Create", "/parent/child", s.builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("", zk.ErrNoNode).Once()
	s.conn.On("Exists", "/parent").Return(false, nil, nil).Once()
	s.conn.On("Create", "/parent", []byte{}, int32(PERSISTENT), zk.WorldACL(zk.PermAll)).Return("/parent", nil).Once()
	s.conn.On("Create", "/parent/child", s.builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("/parent/child", nil).Once()

	path, err := client.Create().CreatingParentsIfNeeded().ForPath("/parent/child")

	assert.Equal(s.T(), "/parent/child", path)
	assert.NoError(s.T(), err)

	assert.NoError(s.T(), client.Close())
}
