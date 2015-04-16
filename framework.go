package curator

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

type CuratorFrameworkState int32

const (
	LATENT  CuratorFrameworkState = iota // CuratorFramework.Start() has not yet been called
	STARTED                              // CuratorFramework.Start() has been called
	STOPPED                              // CuratorFramework.Close() has been called
)

func (s *CuratorFrameworkState) Change(oldState, newState CuratorFrameworkState) bool {
	return atomic.CompareAndSwapInt32((*int32)(s), int32(oldState), int32(newState))
}

func (s *CuratorFrameworkState) Value() CuratorFrameworkState {
	return CuratorFrameworkState(atomic.LoadInt32((*int32)(s)))
}

func (s CuratorFrameworkState) Check(state CuratorFrameworkState, msg string) {
	if s != state {
		panic(msg)
	}
}

const (
	DEFAULT_SESSION_TIMEOUT    time.Duration = 60 * time.Second
	DEFAULT_CONNECTION_TIMEOUT               = 15 * time.Second
)

// Zookeeper framework-style client
type CuratorFramework interface {
	// Start the client.
	// Most mutator methods will not work until the client is started
	Start() error

	// Stop the client
	Close() error

	// Returns the state of this instance
	State() CuratorFrameworkState

	// Return true if the client is started, not closed, etc.
	Started() bool

	// Start a create builder
	Create() CreateBuilder

	// Start a delete builder
	Delete() DeleteBuilder

	// Start an exists builder
	CheckExists() CheckExistsBuilder

	// Start a get data builder
	GetData() GetDataBuilder

	// Start a set data builder
	SetData() SetDataBuilder

	// Start a get children builder
	GetChildren() GetChildrenBuilder

	// Start a get ACL builder
	GetACL() GetACLBuilder

	// Start a set ACL builder
	SetACL() SetACLBuilder

	// Start a transaction builder
	InTransaction() CuratorTransaction

	// Returns the listenable interface for the Connect State
	ConnectionStateListenable() ConnectionStateListenable

	// Returns the listenable interface for events
	CuratorListenable() CuratorListenable

	// Returns the listenable interface for unhandled errors
	UnhandledErrorListenable() UnhandledErrorListenable

	// Return the managed zookeeper client
	ZookeeperClient() *CuratorZookeeperClient
}

func NewClient(connString string, retryPolicy RetryPolicy) CuratorFramework {
	return NewClientTimeout(connString, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, retryPolicy)
}

func NewClientTimeout(connString string, sessionTimeout, connectionTimeout time.Duration, retryPolicy RetryPolicy) CuratorFramework {
	builder := &CuratorFrameworkBuilder{
		ConnectionTimeout: connectionTimeout,
		SessionTimeout:    sessionTimeout,
		RetryPolicy:       retryPolicy,
	}

	return builder.ConnectString(connString).Build()
}

type CuratorFrameworkBuilder struct {
	AuthInfos           []AuthInfo
	ZookeeperDialer     ZookeeperDialer
	EnsembleProvider    EnsembleProvider
	DefaultData         []byte
	Namespace           string
	SessionTimeout      time.Duration
	ConnectionTimeout   time.Duration
	MaxCloseWait        time.Duration
	RetryPolicy         RetryPolicy
	CompressionProvider CompressionProvider
	AclProvider         ACLProvider
	CanBeReadOnly       bool
}

// Apply the current values and build a new CuratorFramework
func (b *CuratorFrameworkBuilder) Build() CuratorFramework {
	return newCuratorFramework(b)
}

// Set the list of servers to connect to.
func (b *CuratorFrameworkBuilder) ConnectString(connectString string) *CuratorFrameworkBuilder {
	b.EnsembleProvider = &fixedEnsembleProvider{connectString}

	return b
}

// Add connection authorization
func (b *CuratorFrameworkBuilder) Authorization(scheme string, auth []byte) *CuratorFrameworkBuilder {
	b.AuthInfos = append(b.AuthInfos, &authInfo{scheme, auth})

	return b
}

type curatorFramework struct {
	client                  *CuratorZookeeperClient
	stateManager            *ConnectionStateManager
	state                   CuratorFrameworkState
	listeners               CuratorListenable
	unhandledErrorListeners UnhandledErrorListenable
	defaultData             []byte
	retryPolicy             RetryPolicy
	compressionProvider     CompressionProvider
	aclProvider             ACLProvider
}

func newCuratorFramework(b *CuratorFrameworkBuilder) *curatorFramework {
	c := &curatorFramework{
		listeners:               NewCuratorListenerContainer(),
		unhandledErrorListeners: NewUnhandledErrorListenerContainer(),
		defaultData:             b.DefaultData,
		retryPolicy:             b.RetryPolicy,
		compressionProvider:     b.CompressionProvider,
		aclProvider:             b.AclProvider,
	}

	watcher := NewWatcher(func(event *zk.Event) {
		c.processEvent(&curatorEvent{
			eventType:    WATCHED,
			err:          event.Err,
			path:         c.unfixForNamespace(event.Path),
			watchedEvent: event,
		})
	})

	c.client = NewCuratorZookeeperClient(b.ZookeeperDialer, b.EnsembleProvider, b.SessionTimeout, b.ConnectionTimeout, watcher, b.RetryPolicy, b.CanBeReadOnly)
	c.stateManager = NewConnectionStateManager(c)

	return c
}

func (c *curatorFramework) Start() error {
	if !c.state.Change(LATENT, STARTED) {
		return fmt.Errorf("Cannot be started more than once")
	} else if err := c.stateManager.Start(); err != nil {
		return fmt.Errorf("fail to start state manager, %s", err)
	} else if err := c.client.Start(); err != nil {
		return fmt.Errorf("fail to start client, %s", err)
	}

	return nil
}

func (c *curatorFramework) Close() error {
	if !c.state.Change(STARTED, STOPPED) {
		return nil
	}

	evt := &curatorEvent{eventType: CLOSING}

	c.listeners.ForEach(func(listener CuratorListener) error {
		return listener.EventReceived(c, evt)
	})

	c.listeners.Clear()
	c.unhandledErrorListeners.Clear()

	if err := c.stateManager.Close(); err != nil {
		glog.Errorf("fail to close state manager, %s", err)
	}

	return c.client.Close()
}

func (c *curatorFramework) State() CuratorFrameworkState {
	return c.state.Value()
}

func (c *curatorFramework) Started() bool {
	return c.State() == STARTED
}

func (c *curatorFramework) Create() CreateBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return &createBuilder{client: c}
}

func (c *curatorFramework) Delete() DeleteBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return &checkExistsBuilder{client: c}
}

func (c *curatorFramework) CheckExists() CheckExistsBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return nil
}

func (c *curatorFramework) GetData() GetDataBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return &getDataBuilder{client: c}
}

func (c *curatorFramework) SetData() SetDataBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return &setDataBuilder{client: c, version: -1}
}

func (c *curatorFramework) GetChildren() GetChildrenBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return nil
}

func (c *curatorFramework) GetACL() GetACLBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return nil
}

func (c *curatorFramework) SetACL() SetACLBuilder {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return nil
}

func (c *curatorFramework) InTransaction() CuratorTransaction {
	c.state.Check(STARTED, "instance must be started before calling this method")

	return nil
}

func (c *curatorFramework) ConnectionStateListenable() ConnectionStateListenable {
	return c.stateManager.listeners
}

func (c *curatorFramework) CuratorListenable() CuratorListenable {
	return c.listeners
}

func (c *curatorFramework) UnhandledErrorListenable() UnhandledErrorListenable {
	return c.unhandledErrorListeners
}

func (c *curatorFramework) ZookeeperClient() *CuratorZookeeperClient {
	return c.client
}

func (c *curatorFramework) processEvent(event CuratorEvent) {
	if event.Type() == WATCHED {

	}

}

func (c *curatorFramework) fixForNamespace(path string, isSequential bool) string {
	return path
}

func (c *curatorFramework) unfixForNamespace(path string) string {
	return path
}

func (c *curatorFramework) getNamespaceWatcher(watcher Watcher) Watcher {
	return watcher
}
