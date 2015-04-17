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

func (c *MockConn) AddAuth(scheme string, auth []byte) error {
	args := c.Called(scheme, auth)

	return args.Error(0)
}

func (c *MockConn) Close() {
	c.Called()
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

func (c *MockConn) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)

	return args.Bool(0), stat, events, args.Error(3)
}

func (c *MockConn) Delete(path string, version int32) error {
	args := c.Called(path, version)

	return args.Error(0)
}

func (c *MockConn) Get(path string) ([]byte, *zk.Stat, error) {
	args := c.Called(path)

	data, _ := args.Get(0).([]byte)
	stat, _ := args.Get(1).(*zk.Stat)

	return data, stat, args.Error(2)
}

func (c *MockConn) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	data, _ := args.Get(0).([]byte)
	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)

	return data, stat, events, args.Error(3)
}

func (c *MockConn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	args := c.Called(path, data, version)

	stat, _ := args.Get(0).(*zk.Stat)

	return stat, args.Error(1)
}

func (c *MockConn) Children(path string) ([]string, *zk.Stat, error) {
	args := c.Called(path)

	children, _ := args.Get(0).([]string)
	stat, _ := args.Get(1).(*zk.Stat)

	return children, stat, args.Error(2)
}

func (c *MockConn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	args := c.Called(path)

	children, _ := args.Get(0).([]string)
	stat, _ := args.Get(1).(*zk.Stat)
	events, _ := args.Get(2).(chan zk.Event)

	return children, stat, events, args.Error(3)
}

func (c *MockConn) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	args := c.Called(path)

	acls, _ := args.Get(0).([]zk.ACL)
	stat, _ := args.Get(1).(*zk.Stat)

	return acls, stat, args.Error(2)
}

func (c *MockConn) SetACL(path string, acls []zk.ACL, version int32) (*zk.Stat, error) {
	args := c.Called(path, acls, version)

	stat, _ := args.Get(0).(*zk.Stat)

	return stat, args.Error(1)
}

func (c *MockConn) Multi(ops ...interface{}) ([]zk.MultiResponse, error) {
	args := c.Called(ops)

	res, _ := args.Get(0).([]zk.MultiResponse)

	return res, args.Error(1)
}

func (c *MockConn) Sync(path string) (string, error) {
	args := c.Called(path)

	return args.String(0), args.Error(1)
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
