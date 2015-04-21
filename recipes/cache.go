package recipes

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/flier/curator.go"
	"github.com/samuel/go-zookeeper/zk"
)

type CacheEventType int

const (
	CHILD_ADDED            CacheEventType = iota // A child was added to the path
	CHILD_UPDATED                                // A child's data was changed
	CHILD_REMOVED                                // A child was removed from the path
	CONNECTION_SUSPENDED                         // Called when the connection has changed to SUSPENDED
	CONNECTION_RECONNECTED                       // Called when the connection has changed to RECONNECTED
	CONNECTION_LOST                              // Called when the connection has changed to LOST
	INITIALIZED                                  // Posted when PathChildrenCache.Start(StartMode) is called with POST_INITIALIZED_EVENT
)

type ChildData struct {
	Path string
	Stat *zk.Stat
	Data []byte
}

type CacheEvent struct {
	Type CacheEventType
	Data ChildData
}

type PathChildrenCacheEvent CacheEvent

type TreeCacheEvent CacheEvent

type NodeCacheListener interface {
	// Called when a change has occurred
	NodeChanged() error
}

type NodeCacheListenable interface {
	curator.Listenable /* [T] */

	Add(listener NodeCacheListener)

	Remove(listener NodeCacheListener)
}

// Listener for PathChildrenCache changes
type PathChildrenCacheListener interface {
	// Called when a change has occurred
	ChildEvent(client curator.CuratorFramework, event PathChildrenCacheEvent) error
}

// Listener for TreeCache changes
type TreeCacheListener interface {
	// Called when a change has occurred
	ChildEvent(client curator.CuratorFramework, event TreeCacheEvent) error
}

type NodeCacheListenerContainer struct {
	*curator.ListenerContainer
}

func (c *NodeCacheListenerContainer) Add(listener NodeCacheListener) {
	c.AddListener(listener)
}

func (c *NodeCacheListenerContainer) Remove(listener NodeCacheListener) {
	c.RemoveListener(listener)
}

// A utility that attempts to keep the data from a node locally cached.
// This class will watch the node, respond to update/create/delete events, pull down the data, etc.
// You can register a listener that will get notified when changes occur.
type NodeCache struct {
	client                  curator.CuratorFramework
	path                    string
	compressed              bool
	state                   curator.State
	isConnected             bool
	data                    *ChildData
	connectionStateListener curator.ConnectionStateListener
	watcher                 curator.Watcher
	backgroundCallback      curator.BackgroundCallback
	listeners               *NodeCacheListenerContainer
}

func NewNodeCache(client curator.CuratorFramework, path string, compressed bool) *NodeCache {
	c := &NodeCache{
		client:     client,
		path:       path,
		compressed: compressed,
		connectionStateListener: curator.NewConnectionStateListener(func(client curator.CuratorFramework, newState curator.ConnectionState) {

		}),
		listeners: &NodeCacheListenerContainer{},
	}

	c.watcher = curator.NewWatcher(func(event *zk.Event) {
		c.reset()
	})
	c.backgroundCallback = func(client curator.CuratorFramework, event curator.CuratorEvent) error {
		return c.processBackgroundResult(event)
	}

	return c
}

// Start the cache. The cache is not started automatically. You must call this method.
func (c *NodeCache) Start() error {
	return c.StartAndInitalize(false)
}

// Same as Start() but gives the option of doing an initial build
func (c *NodeCache) StartAndInitalize(buildInitial bool) error {
	if !c.state.Change(curator.LATENT, curator.STARTED) {
		return fmt.Errorf("Cannot be started more than once")
	}

	c.client.ConnectionStateListenable().Add(c.connectionStateListener)

	if buildInitial {
		if err := c.internalRebuild(); err != nil {
			return err
		}
	}

	return c.reset()
}

func (c *NodeCache) Close() error {
	if c.state.Change(curator.STARTED, curator.STOPPED) {

	}

	c.client.ConnectionStateListenable().Remove(c.connectionStateListener)

	return nil
}

func (c *NodeCache) NodeCacheListenable() NodeCacheListenable {
	return c.listeners
}

func (c *NodeCache) internalRebuild() error {
	var stat zk.Stat

	builder := c.client.GetData()

	if c.compressed {
		builder.Decompressed()
	}

	if data, err := builder.StoringStatIn(&stat).ForPath(c.path); err == nil {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&c.data)), unsafe.Pointer(&ChildData{c.path, &stat, data}))
	} else if err == zk.ErrNoNode {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&c.data)), nil)
	} else {
		return err
	}

	return nil
}

func (c *NodeCache) reset() error {
	if c.state.Value() == curator.STARTED && c.isConnected {
		_, err := c.client.CheckExists().UsingWatcher(c.watcher).InBackgroundWithCallback(c.backgroundCallback).ForPath(c.path)

		return err
	}

	return nil
}

func (c *NodeCache) processBackgroundResult(event curator.CuratorEvent) error {
	switch event.Type() {
	case curator.GET_DATA:
		if event.Err() == nil {
			c.setNewData(&ChildData{c.path, event.Stat(), event.Data()})
		}
	case curator.EXISTS:
		if event.Err() == zk.ErrNoNode {
			c.setNewData(nil)
		} else if event.Err() == nil {
			builder := c.client.GetData()

			if c.compressed {
				builder.Decompressed()
			}

			builder.UsingWatcher(c.watcher).InBackgroundWithCallback(c.backgroundCallback).ForPath(c.path)
		}
	}

	return nil
}

func (c *NodeCache) setNewData(newData *ChildData) {
	previousData := (*ChildData)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&c.data)), unsafe.Pointer(newData)))

	if !reflect.DeepEqual(previousData, newData) {
		c.listeners.ForEach(func(listener interface{}) {
			listener.(NodeCacheListener).NodeChanged()
		})
	}
}
