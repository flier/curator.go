package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

type CreateBuilder interface {
	PathAndBytesable

	// Causes any parent nodes to get created if they haven't already been
	CreatingParentsIfNeeded() CreateBuilder

	// CreateModable
	//
	// Set a create mode - the default is CreateMode.PERSISTENT
	WithMode(mode CreateMode) CreateBuilder

	// ACLable
	//
	// Set an ACL list
	WithACL(acl ...zk.ACL) CreateBuilder

	// Compressible
	//
	// Cause the data to be compressed using the configured compression provider
	Compressed() CreateBuilder

	// Backgroundable
	//
	// Perform the action in the background
	InBackground() CreateBuilder

	// Perform the action in the background
	InBackgroundWithContext(context interface{}) CreateBuilder

	// Perform the action in the background
	InBackgroundWithCallback(callback BackgroundCallback) CreateBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CreateBuilder

	// Perform the action in the background
	InBackgroundWithCallbackAndExecutor(callback BackgroundCallback, executor Executor) CreateBuilder
}

type DeleteBuilder interface {
}

type CheckExistsBuilder interface {
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
	executor     Executor
}

const (
	PROTECTED_PREFIX = "_c_"
)

type createBuilder struct {
	client                *curatorFramework
	createMode            CreateMode
	backgrounding         *backgrounding
	createParentsIfNeeded bool
	compress              bool
	acling                *acling
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
		b.pathInBackground(adjustedPath, payload, givenPath)

		return "", nil
	} else {
		path, err := b.pathInForeground(adjustedPath, payload)

		return b.client.unfixForNamespace(path), err
	}
}

func (b *createBuilder) pathInBackground(adjustedPath string, payload []byte, givenPath string) {

}

func (b *createBuilder) pathInForeground(path string, payload []byte) (string, error) {
	zkClient := b.client.ZookeeperClient()

	result, err := zkClient.newRetryLoop().callWithRetry(func() (result interface{}, err error) {
		result, err = zkClient.Zookeeper().Create(path, payload, int32(b.createMode), b.acling.aclList)

		if err == zk.ErrNoNode && b.createParentsIfNeeded {
			MakeDirs(zkClient.Zookeeper(), path, false, b.acling.aclProvider)

			result, err = zkClient.Zookeeper().Create(path, payload, int32(b.createMode), b.acling.aclList)
		}

		return
	})

	return result.(string), err
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
	b.acling = &acling{aclList: acls, aclProvider: b.client.aclProvider}

	return b
}

func (b *createBuilder) Compressed() CreateBuilder {
	b.compress = true

	return b
}

func (b *createBuilder) InBackground() CreateBuilder {
	b.backgrounding = &backgrounding{inBackground: true}

	return b
}

func (b *createBuilder) InBackgroundWithContext(context interface{}) CreateBuilder {
	b.backgrounding = &backgrounding{inBackground: true, context: context}

	return b
}

func (b *createBuilder) InBackgroundWithCallback(callback BackgroundCallback) CreateBuilder {
	b.backgrounding = &backgrounding{inBackground: true, callback: callback}

	return b
}

func (b *createBuilder) InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) CreateBuilder {
	b.backgrounding = &backgrounding{inBackground: true, context: context, callback: callback}

	return b
}

func (b *createBuilder) InBackgroundWithCallbackAndExecutor(callback BackgroundCallback, executor Executor) CreateBuilder {
	b.backgrounding = &backgrounding{inBackground: true, callback: callback, executor: executor}

	return b
}
