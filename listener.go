package curator

import (
	"fmt"
	"reflect"
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
	// Add the given listener.
	AddListener(listener interface{} /* T */)

	// Remove the given listener
	RemoveListener(listener interface{} /* T */)

	Len() int

	Clear()

	ForEach(fn interface{}, args ...interface{} /* T */) error
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

type listenerContainer struct {
	lock      sync.Mutex
	listeners map[interface{}][]reflect.Value
}

func newListenerContainer() *listenerContainer {
	return &listenerContainer{
		listeners: make(map[interface{}][]reflect.Value),
	}
}

func (c *listenerContainer) AddListener(listener interface{}) {
	c.lock.Lock()

	c.listeners[listener] = nil

	c.lock.Unlock()
}

func (c *listenerContainer) RemoveListener(listener interface{}) {
	c.lock.Lock()

	delete(c.listeners, listener)

	c.lock.Unlock()
}

func (c *listenerContainer) Len() int {
	return len(c.listeners)
}

func (c *listenerContainer) Clear() {
	c.lock.Lock()

	c.listeners = make(map[interface{}][]reflect.Value)

	c.lock.Unlock()
}

func (c *listenerContainer) ForEach(fn interface{}, args ...interface{}) error {
	v := reflect.ValueOf(fn)

	if v.Kind() != reflect.Func {
		return fmt.Errorf("`fn` should be a function, %s", fn)
	}

	var opts []reflect.Value

	for _, arg := range args {
		opts = append(opts, reflect.ValueOf(arg))
	}

	for listener, _ := range c.listeners {
		c.listeners[listener] = v.Call(append([]reflect.Value{reflect.ValueOf(listener)}, opts...))
	}

	return nil
}

type connectionStateListenerContainer struct {
	*listenerContainer
}

func newConnectionStateListenerContainer() *connectionStateListenerContainer {
	return &connectionStateListenerContainer{newListenerContainer()}
}

func (c *connectionStateListenerContainer) Add(listener ConnectionStateListener) {
	c.AddListener(listener)
}

func (c *connectionStateListenerContainer) Remove(listener ConnectionStateListener) {
	c.RemoveListener(listener)
}

type curatorListenerContainer struct {
	*listenerContainer
}

func newCuratorListenerContainer() *curatorListenerContainer {
	return &curatorListenerContainer{newListenerContainer()}
}

func (c *curatorListenerContainer) Add(listener CuratorListener) {
	c.AddListener(listener)
}

func (c *curatorListenerContainer) Remove(listener CuratorListener) {
	c.RemoveListener(listener)
}

type unhandledErrorListenerContainer struct {
	*listenerContainer
}

func newUnhandledErrorListenerContainer() *unhandledErrorListenerContainer {
	return &unhandledErrorListenerContainer{newListenerContainer()}
}

func (c *unhandledErrorListenerContainer) Add(listener UnhandledErrorListener) {
	c.AddListener(listener)
}

func (c *unhandledErrorListenerContainer) Remove(listener UnhandledErrorListener) {
	c.RemoveListener(listener)
}
