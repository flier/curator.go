package curator

import (
	"errors"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
)

type infof func(format string, args ...interface{})

type mockCloseable struct {
	mock.Mock

	crash bool
}

func (c *mockCloseable) Close() error {
	if c.crash {
		panic(errors.New("panic"))
	}

	return c.Called().Error(0)
}

type mockTracerDriver struct {
	mock.Mock
}

func (t *mockTracerDriver) AddTime(name string, d time.Duration) {
	t.Called(name, d)
}

func (t *mockTracerDriver) AddCount(name string, increment int) {
	t.Called(name, increment)
}

type mockRetrySleeper struct {
	mock.Mock
}

func (s *mockRetrySleeper) SleepFor(time time.Duration) error {
	return s.Called(time).Error(0)
}

type mockConn struct {
	mock.Mock

	log infof
}

func (c *mockConn) AddAuth(scheme string, auth []byte) error {
	args := c.Called(scheme, auth)

	return args.Error(0)
}

func (c *mockConn) Close() {
	c.Called()
}

func (c *mockConn) Create(path string, data []byte, flags int32, acls []zk.ACL) (string, error) {
	/*
		if c.log != nil {
			c.log("Before Create(\"%s\", []byte(\"%s\"), %d, %v)", path, data, flags, acls)

			if len(path) == 0 {
				panic(path)
			}
		}
	*/
	args := c.Called(path, data, flags, acls)

	createPath := args.String(0)
	err := args.Error(1)

	if c.log != nil {
		c.log("Create(path=\"%s\", data=[]byte(\"%s\"), flags=%d, alcs=%v) (createdPath=\"%s\", error=%v)", path, data, flags, acls, createPath, err)
	}

	return createPath, err
}

func (c *mockConn) Exists(path string) (bool, *zk.Stat, error) {
	args := c.Called(path)

	exists := args.Bool(0)
	stat, _ := args.Get(1).(*zk.Stat)
	err := args.Error(2)

	if c.log != nil {
		c.log("Exists(path=\"%s\")(exists=%v, stat=%v, error=%v)", path, exists, stat, err)
	}

	return exists, stat, err
}

func (c *mockConn) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	exists := args.Bool(0)
	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)
	err := args.Error(3)

	if c.log != nil {
		c.log("ExistsW(path=\"%s\")(exists=%v, stat=%v, events=%v, error=%v)", path, exists, stat, events, err)
	}

	return exists, stat, events, err
}

func (c *mockConn) Delete(path string, version int32) error {
	args := c.Called(path, version)

	err := args.Error(0)

	if c.log != nil {
		c.log("Delete(path=\"%s\", version=%d) error=%v", path, version, err)
	}

	return err
}

func (c *mockConn) Get(path string) ([]byte, *zk.Stat, error) {
	args := c.Called(path)

	data, _ := args.Get(0).([]byte)
	stat, _ := args.Get(1).(*zk.Stat)

	return data, stat, args.Error(2)
}

func (c *mockConn) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	data, _ := args.Get(0).([]byte)
	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)

	return data, stat, events, args.Error(3)
}

func (c *mockConn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	args := c.Called(path, data, version)

	stat, _ := args.Get(0).(*zk.Stat)

	return stat, args.Error(1)
}

func (c *mockConn) Children(path string) ([]string, *zk.Stat, error) {
	args := c.Called(path)

	children, _ := args.Get(0).([]string)
	stat, _ := args.Get(1).(*zk.Stat)

	return children, stat, args.Error(2)
}

func (c *mockConn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	children, _ := args.Get(0).([]string)
	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)

	return children, stat, events, args.Error(3)
}

func (c *mockConn) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	args := c.Called(path)

	acls, _ := args.Get(0).([]zk.ACL)
	stat, _ := args.Get(1).(*zk.Stat)

	return acls, stat, args.Error(2)
}

func (c *mockConn) SetACL(path string, acls []zk.ACL, version int32) (*zk.Stat, error) {
	args := c.Called(path, acls, version)

	stat, _ := args.Get(0).(*zk.Stat)

	return stat, args.Error(1)
}

func (c *mockConn) Multi(ops ...interface{}) ([]zk.MultiResponse, error) {
	args := c.Called(ops)

	res, _ := args.Get(0).([]zk.MultiResponse)

	return res, args.Error(1)
}

func (c *mockConn) Sync(path string) (string, error) {
	args := c.Called(path)

	return args.String(0), args.Error(1)
}

type mockZookeeperDialer struct {
	mock.Mock

	log infof
}

func (d *mockZookeeperDialer) Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (ZookeeperConnection, <-chan zk.Event, error) {
	args := d.Called(connString, sessionTimeout, canBeReadOnly)

	conn, _ := args.Get(0).(ZookeeperConnection)
	events, _ := args.Get(1).(chan zk.Event)
	err := args.Error(2)

	if d.log != nil {
		d.log("Dial(connectString=\"%s\", sessionTimeout=%v, canBeReadOnly=%v)(conn=%p, events=%v, error=%v)", connString, sessionTimeout, canBeReadOnly, conn, events, err)
	}

	return conn, events, err
}

type mockCompressionProvider struct {
	mock.Mock

	log infof
}

func (p *mockCompressionProvider) Compress(path string, data []byte) ([]byte, error) {
	args := p.Called(path, data)

	compressedData, _ := args.Get(0).([]byte)
	err := args.Error(1)

	if p.log != nil {
		p.log("Compress(path=\"%s\", data=[]byte(\"%s\"))(compressedData=[]byte(\"%s\"), error=%v)", path, data, compressedData, err)
	}

	return compressedData, err
}

func (p *mockCompressionProvider) Decompress(path string, compressedData []byte) ([]byte, error) {
	args := p.Called(path, compressedData)

	data, _ := args.Get(0).([]byte)
	err := args.Error(1)

	if p.log != nil {
		p.log("Decompress(path=\"%s\", compressedData=[]byte(\"%s\"))(data=[]byte(\"%s\"), error=%v)", path, compressedData, data, err)
	}

	return data, err
}

type mockACLProvider struct {
	mock.Mock
}

func (p *mockACLProvider) GetDefaultAcl() []zk.ACL {
	args := p.Called()

	return args.Get(0).([]zk.ACL)
}

func (p *mockACLProvider) GetAclForPath(path string) []zk.ACL {
	args := p.Called(path)

	return args.Get(0).([]zk.ACL)
}
