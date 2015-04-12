package curator

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
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
	Transaction() CuratorTransaction

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
	return Builder().ConnectString(connString).SessionTimeout(sessionTimeout).ConnectionTimeout(connectionTimeout).RetryPolicy(retryPolicy).Build()
}

type CuratorFrameworkBuilder interface {
	// Apply the current values and build a new CuratorFramework
	Build() CuratorFramework

	// Add connection authorization
	Authorization(scheme string, auth []byte) CuratorFrameworkBuilder

	// Add connection authorization.
	Authorizations(authInfos ...AuthInfo) CuratorFrameworkBuilder

	// Set the list of servers to connect to.
	ConnectString(connectString string) CuratorFrameworkBuilder

	// Set the list ensemble provider.
	EnsembleProvider(ensembleProvider EnsembleProvider) CuratorFrameworkBuilder

	// Sets the data to use when PathAndBytesable.ForPath(String) is used.
	DefaultData(defaultData []byte) CuratorFrameworkBuilder

	// As ZooKeeper is a shared space, users of a given cluster should stay within a pre-defined namespace.
	// If a namespace is set here, all paths will get pre-pended with the namespace
	Namespace(namespace string) CuratorFrameworkBuilder

	// session timeout
	SessionTimeout(sessionTimeout time.Duration) CuratorFrameworkBuilder

	// connection timeout
	ConnectionTimeout(connectionTimeout time.Duration) CuratorFrameworkBuilder

	// time to wait during close to join background threads
	MaxCloseWait(maxCloseWait time.Duration) CuratorFrameworkBuilder

	// retry policy to use
	RetryPolicy(retryPolicy RetryPolicy) CuratorFrameworkBuilder

	// the compression provider
	CompressionProvider(compressionProvider CompressionProvider) CuratorFrameworkBuilder

	// a provider for ACLs
	ACLProvider(aclProvider ACLProvider) CuratorFrameworkBuilder

	// allow ZooKeeper client to enter read only mode in case of a network partition
	CanBeReadOnly(canBeReadOnly bool) CuratorFrameworkBuilder
}

func Builder() CuratorFrameworkBuilder {
	return &curatorFrameworkBuilder{}
}

type curatorFrameworkBuilder struct {
	authInfos           []AuthInfo
	ensembleProvider    EnsembleProvider
	defaultData         []byte
	namespace           string
	sessionTimeout      time.Duration
	connectionTimeout   time.Duration
	maxCloseWait        time.Duration
	retryPolicy         RetryPolicy
	compressionProvider CompressionProvider
	aclProvider         ACLProvider
	canBeReadOnly       bool
}

func (b *curatorFrameworkBuilder) Build() CuratorFramework {
	return newCuratorFramework(b)
}

func (b *curatorFrameworkBuilder) Authorization(scheme string, auth []byte) CuratorFrameworkBuilder {
	return b.Authorizations(&authInfo{scheme, auth})
}

func (b *curatorFrameworkBuilder) Authorizations(authInfos ...AuthInfo) CuratorFrameworkBuilder {
	b.authInfos = append(b.authInfos, authInfos...)

	return b
}

func (b *curatorFrameworkBuilder) ConnectString(connectString string) CuratorFrameworkBuilder {
	b.ensembleProvider = &fixedEnsembleProvider{connectString}

	return b
}

func (b *curatorFrameworkBuilder) EnsembleProvider(ensembleProvider EnsembleProvider) CuratorFrameworkBuilder {
	b.ensembleProvider = ensembleProvider

	return b
}

func (b *curatorFrameworkBuilder) DefaultData(defaultData []byte) CuratorFrameworkBuilder {
	b.defaultData = defaultData

	return b
}

func (b *curatorFrameworkBuilder) Namespace(namespace string) CuratorFrameworkBuilder {
	b.namespace = namespace

	return b
}

func (b *curatorFrameworkBuilder) SessionTimeout(sessionTimeout time.Duration) CuratorFrameworkBuilder {
	b.sessionTimeout = sessionTimeout

	return b
}

func (b *curatorFrameworkBuilder) ConnectionTimeout(connectionTimeout time.Duration) CuratorFrameworkBuilder {
	b.connectionTimeout = connectionTimeout

	return b
}

func (b *curatorFrameworkBuilder) MaxCloseWait(maxCloseWait time.Duration) CuratorFrameworkBuilder {
	b.maxCloseWait = maxCloseWait

	return b
}

func (b *curatorFrameworkBuilder) RetryPolicy(retryPolicy RetryPolicy) CuratorFrameworkBuilder {
	b.retryPolicy = retryPolicy

	return b
}

func (b *curatorFrameworkBuilder) CompressionProvider(compressionProvider CompressionProvider) CuratorFrameworkBuilder {
	b.compressionProvider = compressionProvider

	return b
}

func (b *curatorFrameworkBuilder) ACLProvider(aclProvider ACLProvider) CuratorFrameworkBuilder {
	b.aclProvider = aclProvider

	return b
}

func (b *curatorFrameworkBuilder) CanBeReadOnly(canBeReadOnly bool) CuratorFrameworkBuilder {
	b.canBeReadOnly = canBeReadOnly

	return b
}

type curatorFramework struct {
	client                  *CuratorZookeeperClient
	stateManager            *ConnectionStateManager
	state                   CuratorFrameworkState
	listeners               CuratorListenable
	unhandledErrorListeners UnhandledErrorListenable
	defaultData             []byte
	compressionProvider     CompressionProvider
	aclProvider             ACLProvider
}

func newCuratorFramework(builder *curatorFrameworkBuilder) *curatorFramework {
	c := &curatorFramework{
		client:                  NewCuratorZookeeperClient(),
		listeners:               NewCuratorListenerContainer(),
		unhandledErrorListeners: NewUnhandledErrorListenerContainer(),
		defaultData:             builder.defaultData,
		compressionProvider:     builder.compressionProvider,
		aclProvider:             builder.aclProvider,
	}

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
	return &createBuilder{client: c}
}

func (c *curatorFramework) Delete() DeleteBuilder {
	return nil
}

func (c *curatorFramework) CheckExists() CheckExistsBuilder {
	return nil
}

func (c *curatorFramework) GetData() GetDataBuilder {
	return nil
}

func (c *curatorFramework) SetData() SetDataBuilder {
	return nil
}

func (c *curatorFramework) GetChildren() GetChildrenBuilder {
	return nil
}

func (c *curatorFramework) GetACL() GetACLBuilder {
	return nil
}

func (c *curatorFramework) SetACL() SetACLBuilder {
	return nil
}

func (c *curatorFramework) Transaction() CuratorTransaction {
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

func (c *curatorFramework) fixForNamespace(path string, isSequential bool) string {
	return path
}

func (c *curatorFramework) unfixForNamespace(path string) string {
	return path
}
