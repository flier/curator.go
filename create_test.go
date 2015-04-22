package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateBuilderTestSuite struct {
	mockClientTestSuite
}

func TestCreateBuilder(t *testing.T) {
	suite.Run(t, new(CreateBuilderTestSuite))
}

func (s *CreateBuilderTestSuite) TestCreate() {
	s.WithClient(func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn) {
		acls := zk.WorldACL(zk.PermAll)

		conn.On("Create", "/node", builder.DefaultData, int32(EPHEMERAL), acls).Return("/node", nil).Once()

		path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("/node")

		assert.Equal(s.T(), "/node", path)
		assert.NoError(s.T(), err)
	})
}

func (s *CreateBuilderTestSuite) TestNamespace() {
	s.WithClientAndNamespace("parent", func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn) {
		acls := zk.WorldACL(zk.PermAll)

		conn.On("Exists", "/parent").Return(false, nil, nil).Once()
		conn.On("Create", "/parent", []byte{}, int32(PERSISTENT), acls).Return("/parent", nil).Once()
		conn.On("Create", "/parent/child", builder.DefaultData, int32(EPHEMERAL), acls).Return("/parent/child", nil).Once()

		path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("/child")

		assert.Equal(s.T(), "/child", path)
		assert.NoError(s.T(), err)
	})
}

func (s *CreateBuilderTestSuite) TestBackground() {
	s.WithClientAndNamespace("parent", func(client CuratorFramework, conn *mockConn, wg *sync.WaitGroup) {
		data := []byte("data")
		acls := zk.AuthACL(zk.PermRead)
		ctxt := "context"

		conn.On("Exists", "/parent").Return(true, nil, nil).Once()
		conn.On("Create", "/parent/child", data, int32(PERSISTENT), acls).Return("", zk.ErrAPIError).Once()

		path, err := client.Create().WithACL(acls...).InBackgroundWithCallbackAndContext(
			func(client CuratorFramework, event CuratorEvent) error {
				defer wg.Done()

				assert.Equal(s.T(), CREATE, event.Type())
				assert.Equal(s.T(), "/child", event.Path())
				assert.Equal(s.T(), data, event.Data())
				assert.Equal(s.T(), acls, event.ACLs())
				assert.EqualError(s.T(), event.Err(), zk.ErrAPIError.Error())
				assert.Equal(s.T(), "child", event.Name())
				assert.Equal(s.T(), ctxt, event.Context())

				return nil
			}, ctxt).ForPathWithData("/child", data)

		assert.Equal(s.T(), "/child", path)
		assert.NoError(s.T(), err)
	})
}

func (s *CreateBuilderTestSuite) TestCompression() {
	s.WithClient(func(client CuratorFramework, conn *mockConn, compress *mockCompressionProvider) {
		data := []byte("data")
		compressedData := []byte("compressedData")
		acls := zk.WorldACL(zk.PermAll)

		compress.On("Compress", "/node", data).Return(compressedData, nil).Once()
		conn.On("Create", "/node", compressedData, int32(PERSISTENT), acls).Return("/node", nil).Once()

		path, err := client.Create().Compressed().WithACL(acls...).ForPathWithData("/node", data)

		assert.Equal(s.T(), "/node", path)
		assert.NoError(s.T(), err)
	})
}

func (s *CreateBuilderTestSuite) TestCreateParents() {
	s.WithClient(func(builder *CuratorFrameworkBuilder, client CuratorFramework, conn *mockConn) {
		conn.On("Create", "/parent/child", builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("", zk.ErrNoNode).Once()
		conn.On("Exists", "/parent").Return(false, nil, nil).Once()
		conn.On("Create", "/parent", []byte{}, int32(PERSISTENT), zk.WorldACL(zk.PermAll)).Return("/parent", nil).Once()
		conn.On("Create", "/parent/child", builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("/parent/child", nil).Once()

		path, err := client.Create().CreatingParentsIfNeeded().ForPath("/parent/child")

		assert.Equal(s.T(), "/parent/child", path)
		assert.NoError(s.T(), err)
	})
}
