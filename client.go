package curator

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type CuratorZookeeperClient struct {
	state             *ZookeeperConnectionState
	retryPolicy       RetryPolicy
	connectionTimeout time.Duration
	tracer            TracerDriver
}

func NewCuratorZookeeperClient(ensembleProvider EnsembleProvider, sessionTimeout, connectionTimeout time.Duration, retryPolicy RetryPolicy, canReadOnly bool) *CuratorZookeeperClient {
	return &CuratorZookeeperClient{
		state:             newZookeeperConnectionState(ensembleProvider, sessionTimeout, connectionTimeout, retryPolicy, canReadOnly),
		retryPolicy:       retryPolicy,
		connectionTimeout: connectionTimeout,
		tracer:            newDefaultTracerDriver(),
	}
}

func (c *CuratorZookeeperClient) Start() error {
	return nil
}

func (c *CuratorZookeeperClient) Close() error {
	return nil
}

func (c *CuratorZookeeperClient) newRetryLoop() *retryLoop {
	return newRetryLoop(c.retryPolicy, c.tracer)
}

func (c *CuratorZookeeperClient) Conn() *zk.Conn {
	return c.state.conn
}

type ZookeeperConnectionState struct {
	conn *zk.Conn
}

func newZookeeperConnectionState(ensembleProvider EnsembleProvider, sessionTimeout, connectionTimeout time.Duration, retryPolicy RetryPolicy, canReadOnly bool) *ZookeeperConnectionState {
	return &ZookeeperConnectionState{}
}
