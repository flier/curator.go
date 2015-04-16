package curator

import (
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
	// Pathable
	//
	// Commit the currently building operation using the given path
	ForPath(path string) ([]byte, error)

	// Decompressible[T]
	//
	// Cause the data to be de-compressed using the configured compression provider
	Decompressed() GetDataBuilder

	// Statable[T]
	//
	// Have the operation fill the provided stat object
	StoringStatIn(stat *zk.Stat) GetDataBuilder

	// Watchable[T]
	//
	// Have the operation set a watch
	Watched() GetDataBuilder

	// Set a watcher for the operation
	UsingWatcher(watcher Watcher) GetDataBuilder

	// Backgroundable[T]
	//
	// Perform the action in the background
	InBackground() GetDataBuilder

	// Perform the action in the background
	InBackgroundWithContext(context interface{}) GetDataBuilder

	// Perform the action in the background
	InBackgroundWithCallback(callback BackgroundCallback) GetDataBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) GetDataBuilder
}

type SetDataBuilder interface {
	// PathAndBytesable
	//
	// Commit the currently building operation using the given path
	ForPath(path string) (*zk.Stat, error)

	// Commit the currently building operation using the given path and data
	ForPathWithData(path string, payload []byte) (*zk.Stat, error)

	// Versionable[T]
	//
	// Use the given version (the default is -1)
	WithVersion(version int) SetDataBuilder

	// Compressible[T]
	//
	// Cause the data to be compressed using the configured compression provider
	Compressed() SetDataBuilder

	// Backgroundable[T]
	//
	// Perform the action in the background
	InBackground() SetDataBuilder

	// Perform the action in the background
	InBackgroundWithContext(context interface{}) SetDataBuilder

	// Perform the action in the background
	InBackgroundWithCallback(callback BackgroundCallback) SetDataBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) SetDataBuilder
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
