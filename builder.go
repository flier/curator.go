package curator

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

type CreateBuilder interface {
	// PathAndBytesable
	//
	// Commit the currently building operation using the given path
	ForPath(path string) (string, error)

	// Commit the currently building operation using the given path and data
	ForPathWithData(path string, payload []byte) (string, error)

	// Causes any parent nodes to get created if they haven't already been
	CreatingParentsIfNeeded() CreateBuilder

	// CreateModable[T]
	//
	// Set a create mode - the default is CreateMode.PERSISTENT
	WithMode(mode CreateMode) CreateBuilder

	// ACLable[T]
	//
	// Set an ACL list
	WithACL(acl ...zk.ACL) CreateBuilder

	// Compressible[T]
	//
	// Cause the data to be compressed using the configured compression provider
	Compressed() CreateBuilder

	// Backgroundable[T]
	//
	// Perform the action in the background
	InBackground() CreateBuilder

	// Perform the action in the background
	InBackgroundWithContext(context interface{}) CreateBuilder

	// Perform the action in the background
	InBackgroundWithCallback(callback BackgroundCallback) CreateBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CreateBuilder
}

type CheckExistsBuilder interface {
	// Pathable[T]
	//
	// Commit the currently building operation using the given path
	ForPath(path string) (*zk.Stat, error)

	// Watchable[T]
	//
	// Have the operation set a watch
	Watched() CheckExistsBuilder

	// Set a watcher for the operation
	UsingWatcher(watcher Watcher) CheckExistsBuilder

	// Backgroundable[T]
	//
	// Perform the action in the background
	InBackground() CheckExistsBuilder

	// Perform the action in the background
	InBackgroundWithContext(context interface{}) CheckExistsBuilder

	// Perform the action in the background
	InBackgroundWithCallback(callback BackgroundCallback) CheckExistsBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CheckExistsBuilder
}

type DeleteBuilder interface {
}

type GetDataBuilder interface {
}

type SetDataBuilder interface {
}

type GetChildrenBuilder interface {
}

type GetACLBuilder interface {
}

type SetACLBuilder interface {
}

type TransactionCreateBuilder interface {
}

type TransactionDeleteBuilder interface {
}

type TransactionSetDataBuilder interface {
}

type TransactionCheckBuilder interface {
}

type acling struct {
	aclList     []zk.ACL
	aclProvider ACLProvider
}

type backgrounding struct {
	inBackground bool
	context      interface{}
	callback     BackgroundCallback
}

type watching struct {
	watcher Watcher
	watched bool
}

type createBuilder struct {
	client                *curatorFramework
	createMode            CreateMode
	backgrounding         backgrounding
	createParentsIfNeeded bool
	compress              bool
	acling                acling
}

func (b *createBuilder) ForPath(path string) (string, error) {
	return b.ForPathWithData(path, b.client.defaultData)
}

func (b *createBuilder) ForPathWithData(givenPath string, payload []byte) (string, error) {
	if b.compress {
		if data, err := b.client.compressionProvider.Compress(givenPath, payload); err != nil {
			return "", err
		} else {
			payload = data
		}
	}

	adjustedPath := b.client.fixForNamespace(givenPath, b.createMode.IsSequential())

	if b.backgrounding.inBackground {
		go b.pathInBackground(adjustedPath, payload, givenPath)

		return b.client.unfixForNamespace(adjustedPath), nil
	} else {
		path, err := b.pathInForeground(adjustedPath, payload)

		return b.client.unfixForNamespace(path), err
	}
}

func (b *createBuilder) pathInBackground(path string, payload []byte, givenPath string) {
	tracer := b.client.ZookeeperClient().startTracer("createBuilder.pathInBackground")

	defer tracer.Commit()

	createdPath, err := b.pathInForeground(path, payload)

	event := &curatorEvent{
		eventType: CREATE,
		err:       err,
		path:      createdPath,
		data:      payload,
		context:   b.backgrounding.context,
	}

	if err != nil {
		event.path = givenPath
	} else {
		event.name = GetNodeFromPath(createdPath)
	}

	if b.backgrounding.callback != nil {
		b.backgrounding.callback(b.client, event)
	} else if glog.V(3) {
		glog.V(3).Infof("ignore CREATE event: %s", event)
	}
}

func (b *createBuilder) pathInForeground(path string, payload []byte) (string, error) {
	zkClient := b.client.ZookeeperClient()

	result, err := zkClient.newRetryLoop().callWithRetry(func() (interface{}, error) {
		if conn, err := zkClient.Conn(); err != nil {
			return nil, err
		} else {
			result, err := conn.Create(path, payload, int32(b.createMode), b.acling.aclList)

			if err == zk.ErrNoNode && b.createParentsIfNeeded {
				MakeDirs(conn, path, false, b.acling.aclProvider)

				return conn.Create(path, payload, int32(b.createMode), b.acling.aclList)
			} else {
				return result, err
			}
		}
	})

	if err != nil {
		return "", err
	} else if createdPath, ok := result.(string); !ok {
		return "", fmt.Errorf("fail to convert result to string, %s", result)
	} else {
		return createdPath, nil
	}
}

func (b *createBuilder) CreatingParentsIfNeeded() CreateBuilder {
	b.createParentsIfNeeded = true

	return b
}

func (b *createBuilder) WithMode(mode CreateMode) CreateBuilder {
	b.createMode = mode

	return b
}

func (b *createBuilder) WithACL(acls ...zk.ACL) CreateBuilder {
	b.acling = acling{aclList: acls, aclProvider: b.client.aclProvider}

	return b
}

func (b *createBuilder) Compressed() CreateBuilder {
	b.compress = true

	return b
}

func (b *createBuilder) InBackground() CreateBuilder {
	b.backgrounding = backgrounding{inBackground: true}

	return b
}

func (b *createBuilder) InBackgroundWithContext(context interface{}) CreateBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context}

	return b
}

func (b *createBuilder) InBackgroundWithCallback(callback BackgroundCallback) CreateBuilder {
	b.backgrounding = backgrounding{inBackground: true, callback: callback}

	return b
}

func (b *createBuilder) InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CreateBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context, callback: callback}

	return b
}

type checkExistsBuilder struct {
	client        *curatorFramework
	backgrounding backgrounding
	watching      watching
}

func (b *checkExistsBuilder) ForPath(givenPath string) (*zk.Stat, error) {
	adjustedPath := b.client.fixForNamespace(givenPath, false)

	if b.backgrounding.inBackground {
		go b.pathInBackground(adjustedPath)

		return nil, nil
	} else {
		return b.pathInForeground(adjustedPath)
	}
}

func (b *checkExistsBuilder) pathInBackground(path string) {
	tracer := b.client.ZookeeperClient().startTracer("checkExistsBuilder.pathInBackground")

	defer tracer.Commit()

	stat, err := b.pathInForeground(path)

	event := &curatorEvent{
		eventType: EXISTS,
		err:       err,
		path:      path,
		stat:      stat,
		context:   b.backgrounding.context,
	}

	if b.backgrounding.callback != nil {
		b.backgrounding.callback(b.client, event)
	} else if glog.V(3) {
		glog.V(3).Infof("ignore EXISTS event: %s", event)
	}
}

func (b *checkExistsBuilder) pathInForeground(path string) (*zk.Stat, error) {
	zkClient := b.client.ZookeeperClient()

	result, err := zkClient.newRetryLoop().callWithRetry(func() (interface{}, error) {
		if conn, err := zkClient.Conn(); err != nil {
			return nil, err
		} else {
			var exists bool
			var stat *zk.Stat
			var events <-chan zk.Event
			var err error

			if b.watching.watched || b.watching.watcher != nil {
				exists, stat, events, err = conn.ExistsW(path)

				if b.watching.watcher != nil {
					NewWatchers(b.watching.watcher).Watch(events)
				}
			} else {
				exists, stat, err = conn.Exists(path)
			}

			if err != nil {
				return nil, err
			} else if !exists {
				return stat, zk.ErrNoNode
			} else {
				return stat, nil
			}
		}
	})

	stat, _ := result.(*zk.Stat)

	return stat, err
}

func (b *checkExistsBuilder) Watched() CheckExistsBuilder {
	b.watching.watched = true

	return b
}

func (b *checkExistsBuilder) UsingWatcher(watcher Watcher) CheckExistsBuilder {
	b.watching.watcher = b.client.getNamespaceWatcher(watcher)

	return b
}

func (b *checkExistsBuilder) InBackground() CheckExistsBuilder {
	b.backgrounding = backgrounding{inBackground: true}

	return b
}

func (b *checkExistsBuilder) InBackgroundWithContext(context interface{}) CheckExistsBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context}

	return b
}

func (b *checkExistsBuilder) InBackgroundWithCallback(callback BackgroundCallback) CheckExistsBuilder {
	b.backgrounding = backgrounding{inBackground: true, callback: callback}

	return b
}

func (b *checkExistsBuilder) InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CheckExistsBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context, callback: callback}

	return b
}
