package curator

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
)

type CreateBuilder interface {
	PathAndBytesable

	// Causes any parent nodes to get created if they haven't already been
	CreatingParentsIfNeeded() CreateBuilder

	/**
	 * <p>
	 *     Hat-tip to https://github.com/sbridges for pointing this out
	 * </p>
	 *
	 * <p>
	 *     It turns out there is an edge case that exists when creating sequential-ephemeral
	 *     nodes. The creation can succeed on the server, but the server can crash before
	 *     the created node name is returned to the client. However, the ZK session is still
	 *     valid so the ephemeral node is not deleted. Thus, there is no way for the client to
	 *     determine what node was created for them.
	 * </p>
	 *
	 * <p>
	 *     Even without sequential-ephemeral, however, the create can succeed on the sever
	 *     but the client (for various reasons) will not know it.
	 * </p>
	 *
	 * <p>
	 *     Putting the create builder into protection mode works around this.
	 *     The name of the node that is created is prefixed with a GUID. If node creation fails
	 *     the normal retry mechanism will occur. On the retry, the parent path is first searched
	 *     for a node that has the GUID in it. If that node is found, it is assumed to be the lost
	 *     node that was successfully created on the first try and is returned to the caller.
	 * </p>
	 */
	WithProtection() CreateBuilder

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
	protectedId           string
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

	adjustedPath := b.adjustPath(b.client.fixForNamespace(givenPath, b.createMode.IsSequential()))

	if b.backgrounding.inBackground {
		b.pathInBackground(adjustedPath, payload, givenPath)

		return "", nil
	} else {
		path := b.protectedPathInForeground(adjustedPath, payload)

		return b.client.unfixForNamespace(path), nil
	}
}

func (b *createBuilder) adjustPath(path string) string {
	if len(b.protectedId) > 0 {
		pathAndNode := NewPathAndNode(path)
		name := b.protectedPrefix(b.protectedId) + pathAndNode.Node
		return MakePath(pathAndNode.Path, name)
	}

	return path
}

func (b *createBuilder) protectedPrefix(protectedId string) string {
	return PROTECTED_PREFIX + protectedId + "-"
}

func (b *createBuilder) pathInBackground(adjustedPath string, payload []byte, givenPath string) {

}

func (b *createBuilder) protectedPathInForeground(adjustedPath string, payload []byte) string {
	return adjustedPath
}

func (b *createBuilder) CreatingParentsIfNeeded() CreateBuilder {
	b.createParentsIfNeeded = true

	return b
}

func (b *createBuilder) WithProtection() CreateBuilder {
	b.protectedId = uuid.NewV4().String()

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
