package curator

import (
	"fmt"
	"reflect"
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

// Abstracts a listenable object
type Listenable interface {
	// Add the given listener.
	AddListener(listener interface{}, executor Executor)

	// Remove the given listener
	RemoveListener(listener interface{})

	Len() int

	Clear()

	ForEach(fn interface{}, args ...interface{}) error
}

type ConnectionStateListenable interface {
	Listenable

	Add(listener ConnectionStateListener)

	Remove(listener ConnectionStateListener)
}

type CuratorListenable interface {
	Listenable

	Add(listener CuratorListener)

	Remove(listener CuratorListener)
}

type UnhandledErrorListenable interface {
	Listenable

	Add(listener UnhandledErrorListener)

	Remove(listener UnhandledErrorListener)
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

type ListenerContainer struct {
	listeners map[interface{}]Executor
}

func NewListenerContainer() *ListenerContainer {
	return &ListenerContainer{
		listeners: make(map[interface{}]Executor),
	}
}

func (c *ListenerContainer) AddListener(listener interface{}, executor Executor) {
	c.listeners[listener] = executor
}

func (c *ListenerContainer) RemoveListener(listener interface{}) {
	delete(c.listeners, listener)
}

func (c *ListenerContainer) Len() int {
	return len(c.listeners)
}

func (c *ListenerContainer) Clear() {
	c.listeners = make(map[interface{}]Executor)
}

func (c *ListenerContainer) Execute(command Runnable) error {
	return command()
}

func (c *ListenerContainer) ForEach(fn interface{}, args ...interface{}) error {
	v := reflect.ValueOf(fn)

	if v.Kind() != reflect.Func {
		return fmt.Errorf("`fn` should be a function, %s", fn)
	}

	var opts []reflect.Value

	for _, arg := range args {
		opts = append(opts, reflect.ValueOf(arg))
	}

	for listener, executor := range c.listeners {
		if executor == nil {
			executor = c
		}

		if err := executor.Execute(func() error {
			out := v.Call(append([]reflect.Value{reflect.ValueOf(listener)}, opts...))

			if len(out) > 1 {
				if err, ok := out[0].Interface().(error); ok {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

type ConnectionStateListenerContainer struct {
	*ListenerContainer
}

func NewConnectionStateListenerContainer() *ConnectionStateListenerContainer {
	return &ConnectionStateListenerContainer{NewListenerContainer()}
}

func (c *ConnectionStateListenerContainer) Add(listener ConnectionStateListener) {
	c.AddListener(listener, nil)
}

func (c *ConnectionStateListenerContainer) Remove(listener ConnectionStateListener) {
	c.RemoveListener(listener)
}

type CuratorListenerContainer struct {
	*ListenerContainer
}

func NewCuratorListenerContainer() *CuratorListenerContainer {
	return &CuratorListenerContainer{NewListenerContainer()}
}

func (c *CuratorListenerContainer) Add(listener CuratorListener) {
	c.AddListener(listener, nil)
}

func (c *CuratorListenerContainer) Remove(listener CuratorListener) {
	c.RemoveListener(listener)
}

type UnhandledErrorListenerContainer struct {
	*ListenerContainer
}

func NewUnhandledErrorListenerContainer() *UnhandledErrorListenerContainer {
	return &UnhandledErrorListenerContainer{NewListenerContainer()}
}

func (c *UnhandledErrorListenerContainer) Add(listener UnhandledErrorListener) {
	c.AddListener(listener, nil)
}

func (c *UnhandledErrorListenerContainer) Remove(listener UnhandledErrorListener) {
	c.RemoveListener(listener)
}
