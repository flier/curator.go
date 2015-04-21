package curator

import (
	"sync"
)

type ConnectionStateListener interface {
	// Called when there is a state change in the connection
	StateChanged(client CuratorFramework, newState ConnectionState)
}

// Receives notifications about errors and background events
type CuratorListener interface {
	// Called when a background task has completed or a watch has triggered
	EventReceived(client CuratorFramework, event CuratorEvent) error
}

type UnhandledErrorListener interface {
	// Called when an exception is caught in a background thread, handler, etc.
	UnhandledError(err error)
}

type connectionStateListenerCallback func(client CuratorFramework, newState ConnectionState)

type connectionStateListenerStub struct {
	callback connectionStateListenerCallback
}

func NewConnectionStateListener(callback connectionStateListenerCallback) ConnectionStateListener {
	return &connectionStateListenerStub{callback}
}

func (l *connectionStateListenerStub) StateChanged(client CuratorFramework, newState ConnectionState) {
	l.callback(client, newState)
}

type curatorListenerCallback func(client CuratorFramework, event CuratorEvent) error

type curatorListenerStub struct {
	callback curatorListenerCallback
}

func NewCuratorListener(callback curatorListenerCallback) CuratorListener {
	return &curatorListenerStub{callback}
}

func (l *curatorListenerStub) EventReceived(client CuratorFramework, event CuratorEvent) error {
	return l.callback(client, event)
}

type unhandledErrorListenerCallback func(err error)

type unhandledErrorListenerStub struct {
	callback unhandledErrorListenerCallback
}

func NewUnhandledErrorListener(callback unhandledErrorListenerCallback) UnhandledErrorListener {
	return &unhandledErrorListenerStub{callback}
}

func (l *unhandledErrorListenerStub) UnhandledError(err error) {
	l.callback(err)
}

// Abstracts a listenable object
type Listenable /* [T] */ interface {
	Len() int

	Clear()

	ForEach(callback func(interface{}))
}

type ConnectionStateListenable interface {
	Listenable /* [T] */

	Add(listener ConnectionStateListener)

	Remove(listener ConnectionStateListener)
}

type CuratorListenable interface {
	Listenable /* [T] */

	Add(listener CuratorListener)

	Remove(listener CuratorListener)
}

type UnhandledErrorListenable interface {
	Listenable /* [T] */

	Add(listener UnhandledErrorListener)

	Remove(listener UnhandledErrorListener)
}

type ListenerContainer struct {
	lock      sync.RWMutex
	listeners []interface{}
}

func (c *ListenerContainer) AddListener(listener interface{}) {
	c.lock.Lock()

	c.listeners = append(c.listeners, listener)

	c.lock.Unlock()
}

func (c *ListenerContainer) RemoveListener(listener interface{}) {
	c.lock.Lock()

	for i, l := range c.listeners {
		if l == listener {
			copy(c.listeners[i:], c.listeners[i+1:])
			c.listeners = c.listeners[:len(c.listeners)-1]
			break
		}
	}

	c.lock.Unlock()
}

func (c *ListenerContainer) Len() int {
	return len(c.listeners)
}

func (c *ListenerContainer) Clear() {
	c.lock.Lock()

	c.listeners = nil

	c.lock.Unlock()
}

func (c *ListenerContainer) ForEach(callback func(interface{})) {
	c.lock.RLock()

	for _, listener := range c.listeners {
		callback(listener)
	}

	c.lock.RUnlock()
}

type connectionStateListenerContainer struct {
	*ListenerContainer
}

func (c *connectionStateListenerContainer) Add(listener ConnectionStateListener) {
	c.AddListener(listener)
}

func (c *connectionStateListenerContainer) Remove(listener ConnectionStateListener) {
	c.RemoveListener(listener)
}

type curatorListenerContainer struct {
	*ListenerContainer
}

func (c *curatorListenerContainer) Add(listener CuratorListener) {
	c.AddListener(listener)
}

func (c *curatorListenerContainer) Remove(listener CuratorListener) {
	c.RemoveListener(listener)
}

type unhandledErrorListenerContainer struct {
	*ListenerContainer
}

func (c *unhandledErrorListenerContainer) Add(listener UnhandledErrorListener) {
	c.AddListener(listener)
}

func (c *unhandledErrorListenerContainer) Remove(listener UnhandledErrorListener) {
	c.RemoveListener(listener)
}
