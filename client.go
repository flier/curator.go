package curator

import (
	"errors"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

// Allocate a new ZooKeeper connection
type ZookeeperDialer interface {
	Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (*zk.Conn, <-chan zk.Event, error)
}

type DefaultZookeeperDialer struct {
	Dialer zk.Dialer
}

func (d *DefaultZookeeperDialer) Dial(connString string, sessionTimeout time.Duration, canBeReadOnly bool) (*zk.Conn, <-chan zk.Event, error) {
	return zk.ConnectWithDialer(strings.Split(connString, ","), sessionTimeout, d.Dialer)
}

type CuratorZookeeperClient struct {
	state             *ZookeeperConnectionState
	connectionTimeout time.Duration
	watcher           Watcher
	started           bool
	TracerDriver      TracerDriver
	RetryPolicy       RetryPolicy
}

func NewCuratorZookeeperClient(zookeeperDialer ZookeeperDialer, ensembleProvider EnsembleProvider, sessionTimeout, connectionTimeout time.Duration,
	watcher Watcher, retryPolicy RetryPolicy, canReadOnly bool) *CuratorZookeeperClient {

	if sessionTimeout < connectionTimeout {
		glog.Warningf("session timeout [%d] is less than connection timeout [%d]", sessionTimeout, connectionTimeout)
	}

	tracer := newDefaultTracerDriver()

	return &CuratorZookeeperClient{
		state:             newZookeeperConnectionState(zookeeperDialer, ensembleProvider, sessionTimeout, connectionTimeout, watcher, tracer, canReadOnly),
		connectionTimeout: connectionTimeout,
		TracerDriver:      tracer,
		RetryPolicy:       retryPolicy,
	}
}

func (c *CuratorZookeeperClient) Start() error {
	if c.started {
		return errors.New("Already started")
	}

	return c.state.Start()
}

func (c *CuratorZookeeperClient) Close() error {
	c.started = false

	return c.state.Close()
}

// Returns true if the client is current connected
func (c *CuratorZookeeperClient) IsConnected() bool {
	return c.state.isConnected()
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

func (c *CuratorZookeeperClient) Conn() (*zk.Conn, error) {
	return c.state.Conn()
}

// This method blocks until the connection to ZK succeeds.
func (c *CuratorZookeeperClient) BlockUntilConnectedOrTimedOut() error {
	if !c.started {
		return errors.New("Client is not started")
	}

	tracer := c.startTracer("blockUntilConnectedOrTimedOut")

	defer tracer.Commit()

	c.internalBlockUntilConnectedOrTimedOut()

	if c.state.isConnected() {
		return nil
	}

	return errors.New("Client connect timeouted")
}

func (c *CuratorZookeeperClient) internalBlockUntilConnectedOrTimedOut() error {
	timer := time.NewTimer(c.connectionTimeout)
	connected := make(chan error)

	watcher := c.state.addParentWatcher(func(*zk.Event) {
		if c.state.isConnected() {
			connected <- nil
		}
	})

	defer c.state.removeParentWatcher(watcher)

	select {
	case err := <-connected:
		return err
	case <-timer.C:
		return errors.New("Client connect timeouted")
	}
}
