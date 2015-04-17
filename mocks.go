package curator

import (
	"errors"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
)

type MockCloseable struct {
	mock.Mock

	crash bool
}

func (c *MockCloseable) Close() error {
	if c.crash {
		panic(errors.New("panic"))
	}

	return c.Called().Error(0)
}

type MockTracerDriver struct {
	mock.Mock
}

func (t *MockTracerDriver) AddTime(name string, d time.Duration) {
	t.Called(name, d)
}

func (t *MockTracerDriver) AddCount(name string, increment int) {
	t.Called(name, increment)
}

type MockRetrySleeper struct {
	mock.Mock
}

func (s *MockRetrySleeper) SleepFor(time time.Duration) error {
	return s.Called(time).Error(0)
}

type MockConn struct {
	mock.Mock

	ZookeeperConnection
}

func (c *MockConn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	args := c.Called(path, data, flags, acl)

	return args.String(0), args.Error(1)
}

func (c *MockConn) Exists(path string) (bool, *zk.Stat, error) {
	args := c.Called(path)

	stat, _ := args.Get(1).(*zk.Stat)

	return args.Bool(0), stat, args.Error(2)
}

func (c *MockConn) Children(path string) ([]string, *zk.Stat, error) {
	args := c.Called(path)

	children, _ := args.Get(0).([]string)
	stat, _ := args.Get(1).(*zk.Stat)

	return children, stat, args.Error(2)
}

func (c *MockConn) Delete(path string, version int32) error {
	args := c.Called(path, version)

	return args.Error(0)
}

type MockACLProvider struct {
	mock.Mock

	ACLProvider
}

func (p *MockACLProvider) GetDefaultAcl() []zk.ACL {
	args := p.Called()

	return args.Get(0).([]zk.ACL)
}

func (p *MockACLProvider) GetAclForPath(path string) []zk.ACL {
	args := p.Called(path)

	return args.Get(0).([]zk.ACL)
}
