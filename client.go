package curator

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZookeeperConnection interface {
	// Add the specified scheme:auth information to this connection.
	AddAuth(scheme string, auth []byte) error

	// Close this connection
	Close()

	// Create a node with the given path.
	//
	// The node data will be the given data, and node acl will be the given acl.
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)

	// Return the stat of the node of the given path. Return nil if no such a node exists.
	Exists(path string) (bool, *zk.Stat, error)

	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)

	// Delete the node with the given path.
	//
	// The call will succeed if such a node exists,
	// and the given version matches the node's version
	// (if the given version is -1, it matches any node's versions).
	Delete(path string, version int32) error

	// Return the data and the stat of the node of the given path.
	Get(path string) ([]byte, *zk.Stat, error)

	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)

	// Set the ACL for the node of the given path
	// if such a node exists and the given version matches the version of the node.
	// Return the stat of the node.
	Set(path string, data []byte, version int32) (*zk.Stat, error)

	// Return the list of the children of the node of the given path.
	Children(path string) ([]string, *zk.Stat, error)

	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)

	// Return the ACL and stat of the node of the given path.
	GetACL(path string) ([]zk.ACL, *zk.Stat, error)

	// Set the ACL for the node of the given path
	// if such a node exists and the given version matches the version of the node.
	// Return the stat of the node.
	SetACL(path string, acl []zk.ACL, version int32) (*zk.Stat, error)

	// Executes multiple ZooKeeper operations or none of them.
	Multi(ops ...interface{}) ([]zk.MultiResponse, error)

	// Flushes channel between process and leader.
	Sync(path string) (string, error)
}

// Allocate a new ZooKeeper connection
type ZookeeperDialer interface {
	Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (ZookeeperConnection, <-chan zk.Event, error)
}

type ZookeeperDialFunc func(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (ZookeeperConnection, <-chan zk.Event, error)

type zookeeperDialer struct {
	dial ZookeeperDialFunc
}

func (d *zookeeperDialer) Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (ZookeeperConnection, <-chan zk.Event, error) {
	return d.dial(connString, sessionTimeout, canBeReadOnly)
}

func NewZookeeperDialer(dial ZookeeperDialFunc) ZookeeperDialer {
	return &zookeeperDialer{dial}
}

type DefaultZookeeperDialer struct {
	Dialer zk.Dialer
}

func (d *DefaultZookeeperDialer) Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (ZookeeperConnection, <-chan zk.Event, error) {
	return zk.ConnectWithDialer(strings.Split(connString, ","), sessionTimeout, d.Dialer)
}

type CuratorZookeeperClient struct {
	state        *connectionState
	watcher      Watcher
	started      AtomicBool
	TracerDriver TracerDriver
	RetryPolicy  RetryPolicy
}

func NewCuratorZookeeperClient(zookeeperDialer ZookeeperDialer, ensembleProvider EnsembleProvider, sessionTimeout, connectionTimeout time.Duration,
	watcher Watcher, retryPolicy RetryPolicy, canReadOnly bool, authInfos []AuthInfo) *CuratorZookeeperClient {

	if sessionTimeout < connectionTimeout {
		log.Printf("session timeout [%d] is less than connection timeout [%d]", sessionTimeout, connectionTimeout)
	}

	dialer := NewZookeeperDialer(func(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (conn ZookeeperConnection, events <-chan zk.Event, err error) {
		conn, events, err = zookeeperDialer.Dial(connString, sessionTimeout, canBeReadOnly)

		if err == nil && conn != nil {
			for _, authInfo := range authInfos {
				if err := conn.AddAuth(authInfo.Scheme, authInfo.Auth); err != nil {
					conn.Close()

					return nil, nil, err
				}
			}
		}

		return
	})

	tracer := newDefaultTracerDriver()

	return &CuratorZookeeperClient{
		state:        newConnectionState(dialer, ensembleProvider, sessionTimeout, connectionTimeout, watcher, tracer, canReadOnly),
		TracerDriver: tracer,
		RetryPolicy:  retryPolicy,
	}
}

func (c *CuratorZookeeperClient) Start() error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("Already started")
	}

	return c.state.Start()
}

func (c *CuratorZookeeperClient) Close() error {
	c.started.Set(false)

	return c.state.Close()
}

// Returns true if the client is current connected
func (c *CuratorZookeeperClient) IsConnected() bool {
	return c.state.Connected()
}

func (c *CuratorZookeeperClient) CurrentConnectionString() string {
	return c.state.ensembleProvider.ConnectionString()
}

func (c *CuratorZookeeperClient) newRetryLoop() *retryLoop {
	return newRetryLoop(c.RetryPolicy, c.TracerDriver)
}

func (c *CuratorZookeeperClient) startTracer(name string) Tracer {
	return newTimeTracer(name, c.TracerDriver)
}

func (c *CuratorZookeeperClient) Conn() (ZookeeperConnection, error) {
	if !c.started.Load() {
		return nil, errors.New("Client is not started")
	}

	return c.state.Conn()
}

// This method blocks until the connection to ZK succeeds.
func (c *CuratorZookeeperClient) BlockUntilConnectedOrTimedOut() error {
	if !c.started.Load() {
		return errors.New("Client is not started")
	}

	tracer := c.startTracer("blockUntilConnectedOrTimedOut")

	defer tracer.Commit()

	c.internalBlockUntilConnectedOrTimedOut()

	if c.state.Connected() {
		return nil
	}

	return errors.New("Client connect timeouted")
}

func (c *CuratorZookeeperClient) internalBlockUntilConnectedOrTimedOut() error {
	timer := time.NewTimer(c.state.connectionTimeout)
	connected := make(chan error)

	watcher := c.state.AddParentWatcher(NewWatcher(func(*zk.Event) {
		if c.state.Connected() {
			connected <- nil
		}
	}))

	defer c.state.RemoveParentWatcher(watcher)

	select {
	case err := <-connected:
		return err
	case <-timer.C:
		return errors.New("Client connect timeouted")
	}
}
