package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

type CuratorEventType int

const (
	CREATE   CuratorEventType = iota // Corresponds to CuratorFramework.Create()
	DELETE                           // Corresponds to CuratorFramework.Delete()
	EXISTS                           // Corresponds to CuratorFramework.CheckExists()
	GET_DATA                         // Corresponds to CuratorFramework.GetData()
	SET_DATA                         // Corresponds to CuratorFramework.SetData()
	CHILDREN                         // Corresponds to CuratorFramework.GetChildren()
	SYNC                             // Corresponds to CuratorFramework.Sync()
	GET_ACL                          // Corresponds to CuratorFramework.GetACL()
	SET_ACL                          // Corresponds to CuratorFramework.SetACL()
	WATCHED                          // Corresponds to Watchable.UsingWatcher()
	CLOSING                          // Event sent when client is being closed
)

// A super set of all the various Zookeeper events/background methods.
type CuratorEvent interface {
	// check here first - this value determines the type of event and which methods will have valid values
	Type() CuratorEventType

	// "rc" from async callbacks
	ResultCode() int

	// the path
	Path() string

	// the context object passed to Backgroundable.InBackground(interface{})
	Context() interface{}

	// any stat
	Stat() *zk.Stat

	// any data
	Data() []byte

	// any name
	Name() string

	// any children
	Children() []string

	// any ACL list or null
	ACLs() []*zk.ACL

	WatchedEvent() *zk.Event
}

type curatorEvent struct {
	eventType    CuratorEventType
	resultCode   int
	path         string
	name         string
	children     []string
	context      interface{}
	stat         *zk.Stat
	data         []byte
	watchedEvent *zk.Event
	acls         []*zk.ACL
}

func (e *curatorEvent) Type() CuratorEventType { return e.eventType }

func (e *curatorEvent) ResultCode() int { return e.resultCode }

func (e *curatorEvent) Path() string { return e.path }

func (e *curatorEvent) Context() interface{} { return e.context }

func (e *curatorEvent) Stat() *zk.Stat { return e.stat }

func (e *curatorEvent) Data() []byte { return e.data }

func (e *curatorEvent) Name() string { return e.name }

func (e *curatorEvent) Children() []string { return e.children }

func (e *curatorEvent) ACLs() []*zk.ACL { return e.acls }

func (e *curatorEvent) WatchedEvent() *zk.Event { return e.watchedEvent }
