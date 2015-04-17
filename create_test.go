package curator

import (
	"sync"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestCreateBuilder(t *testing.T) {
	conn := &mockConn{}
	dialer := &mockZookeeperDialer{}
	builder := &CuratorFrameworkBuilder{
		ZookeeperDialer: dialer,
	}
	client := builder.ConnectString("connString").Build()
	client.(*curatorFramework).state = STARTED
	acls := zk.WorldACL(zk.PermAll)

	dialer.On("Dial", "connString", builder.ConnectionTimeout, false).Return(conn, nil, nil).Once()
	conn.On("Create", "/node", builder.DefaultData, int32(EPHEMERAL), acls).Return("/node", nil).Once()

	path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("/node")

	assert.Equal(t, "/node", path)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	dialer.AssertExpectations(t)
}

func TestCreateBuilderWithNamespace(t *testing.T) {
	conn := &mockConn{}
	dialer := &mockZookeeperDialer{}
	builder := &CuratorFrameworkBuilder{
		ZookeeperDialer: dialer,
		DefaultData:     []byte("default"),
		Namespace:       "parent",
	}
	client := builder.ConnectString("connString").Build()
	client.(*curatorFramework).state = STARTED
	acls := zk.WorldACL(zk.PermAll)

	dialer.On("Dial", "connString", builder.ConnectionTimeout, false).Return(conn, nil, nil).Once()
	conn.On("Create", "/parent/child", builder.DefaultData, int32(EPHEMERAL), acls).Return("/parent/child", nil).Once()

	path, err := client.Create().WithMode(EPHEMERAL).WithACL(acls...).ForPath("child")

	assert.Equal(t, "/child", path)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	dialer.AssertExpectations(t)
}

func TestCreateBuilderInBackground(t *testing.T) {
	conn := &mockConn{}
	dialer := &mockZookeeperDialer{}
	builder := &CuratorFrameworkBuilder{
		ZookeeperDialer: dialer,
		RetryPolicy:     NewRetryOneTime(0),
	}
	client := builder.ConnectString("connString").Build()
	client.(*curatorFramework).state = STARTED

	data := []byte("data")
	acls := zk.AuthACL(zk.PermRead)
	ctxt := "context"

	dialer.On("Dial", "connString", builder.ConnectionTimeout, false).Return(conn, nil, nil).Once()
	conn.On("Create", "/node", data, int32(PERSISTENT), acls).Return("", zk.ErrAPIError).Once()

	var wg sync.WaitGroup

	wg.Add(1)

	path, err := client.Create().WithACL(acls...).InBackgroundWithCallbackAndContext(
		func(client CuratorFramework, event CuratorEvent) error {
			defer wg.Done()

			assert.Equal(t, CREATE, event.Type())
			assert.Equal(t, "/node", event.Path())
			assert.Equal(t, data, event.Data())
			assert.Equal(t, acls, event.ACLs())
			assert.EqualError(t, event.Err(), zk.ErrAPIError.Error())
			assert.Equal(t, ctxt, event.Context())

			return nil
		}, ctxt).ForPathWithData("/node", data)

	wg.Wait()

	assert.Equal(t, "/node", path)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	dialer.AssertExpectations(t)
}

func TestCreateBuilderWithCompression(t *testing.T) {
	conn := &mockConn{}
	dialer := &mockZookeeperDialer{}
	compress := &mockCompressionProvider{}

	builder := &CuratorFrameworkBuilder{
		ZookeeperDialer:     dialer,
		CompressionProvider: compress,
	}
	client := builder.ConnectString("connString").Build()
	client.(*curatorFramework).state = STARTED

	data := []byte("data")
	compressedData := []byte("compressedData")
	acls := zk.WorldACL(zk.PermAll)

	dialer.On("Dial", "connString", builder.ConnectionTimeout, false).Return(conn, nil, nil).Once()
	compress.On("Compress", "/node", data).Return(compressedData, nil).Once()
	conn.On("Create", "/node", compressedData, int32(PERSISTENT), acls).Return("/node", nil).Once()

	path, err := client.Create().Compressed().WithACL(acls...).ForPathWithData("/node", data)

	assert.Equal(t, "/node", path)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	dialer.AssertExpectations(t)
	compress.AssertExpectations(t)
}

func TestCreateBuilderWithParents(t *testing.T) {
	conn := &mockConn{}
	dialer := &mockZookeeperDialer{}
	builder := &CuratorFrameworkBuilder{
		ZookeeperDialer: dialer,
		Namespace:       "parent",
	}
	client := builder.ConnectString("connString").Build()
	client.(*curatorFramework).state = STARTED

	dialer.On("Dial", "connString", builder.ConnectionTimeout, false).Return(conn, nil, nil).Once()
	conn.On("Create", "/parent/child", builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("", zk.ErrNoNode).Once()
	conn.On("Exists", "/parent").Return(false, nil, nil).Once()
	conn.On("Create", "/parent", []byte{}, int32(PERSISTENT), zk.WorldACL(zk.PermAll)).Return("/parent", nil).Once()
	conn.On("Create", "/parent/child", builder.DefaultData, int32(PERSISTENT), []zk.ACL(nil)).Return("/parent/child", nil).Once()

	path, err := client.Create().CreatingParentsIfNeeded().ForPath("/child")

	assert.Equal(t, "/child", path)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	dialer.AssertExpectations(t)
}
