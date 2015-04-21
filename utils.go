package curator

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/golang/glog"
)

// A Closeable is a source or destination of data that can be closed.
type Closeable interface {
	// Closes this and releases any system resources associated with it.
	Close() error
}

func CloseQuietly(closeable Closeable) (err error) {
	defer func() {
		if v := recover(); v != nil {
			glog.Errorf("panic when closing %s, %v", closeable, v)

			err, _ = v.(error)
		}
	}()

	if err = closeable.Close(); err != nil {
		glog.Errorf("fail to close %s, %s", closeable, err)
	}

	return
}

type AtomicBool int32

const (
	FALSE AtomicBool = iota
	TRUE
)

func NewAtomicBool(b bool) AtomicBool {
	if b {
		return TRUE
	}

	return FALSE
}

func (b *AtomicBool) CompareAndSwap(oldValue, newValue bool) bool {
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(b)), int32(NewAtomicBool(oldValue)), int32(NewAtomicBool(newValue)))
}

func (b *AtomicBool) Load() bool {
	return atomic.LoadInt32((*int32)(unsafe.Pointer(b))) != int32(FALSE)
}

func (b *AtomicBool) Swap(v bool) bool {
	return atomic.SwapInt32((*int32)(unsafe.Pointer(b)), int32(FALSE)) != int32(FALSE)
}

func (b *AtomicBool) Set(v bool) { b.Swap(v) }

type EnsurePath interface {
	// First time, synchronizes and makes sure all nodes in the path are created.
	// Subsequent calls with this instance are NOPs.
	Ensure(client *CuratorZookeeperClient) error

	// Returns a view of this EnsurePath instance that does not make the last node.
	ExcludingLast() EnsurePath
}

type EnsurePathHelper interface {
	Ensure(client *CuratorZookeeperClient, path string, makeLastNode bool) error
}

type ensurePathHelper struct {
	owner   *ensurePath
	lock    sync.Mutex
	started bool
}

func (h *ensurePathHelper) Ensure(client *CuratorZookeeperClient, path string, makeLastNode bool) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.started {
		_, err := client.newRetryLoop().CallWithRetry(func() (interface{}, error) {
			if conn, err := client.Conn(); err != nil {
				return nil, err
			} else if err := MakeDirs(conn, h.owner.path, h.owner.makeLastNode, h.owner.aclProvider); err != nil {
				return nil, err
			} else {
				return nil, nil
			}
		})

		h.owner.helper.Store(nil)

		h.started = true

		return err
	}

	return nil
}

// Utility to ensure that a particular path is created.
type ensurePath struct {
	path         string
	aclProvider  ACLProvider
	makeLastNode bool
	helper       atomic.Value
}

func NewEnsurePath(path string) *ensurePath {
	return NewEnsurePathWithAclAndHelper(path, nil, nil)
}

func NewEnsurePathWithAcl(path string, aclProvider ACLProvider) *ensurePath {
	return NewEnsurePathWithAclAndHelper(path, aclProvider, nil)
}

func NewEnsurePathWithAclAndHelper(path string, aclProvider ACLProvider, helper EnsurePathHelper) *ensurePath {
	p := &ensurePath{
		path:         path,
		aclProvider:  aclProvider,
		makeLastNode: true,
	}

	if helper == nil {
		p.helper.Store(&ensurePathHelper{owner: p})
	} else {
		p.helper.Store(helper)
	}

	return p
}

func (p *ensurePath) ExcludingLast() EnsurePath {
	return &ensurePath{
		path:         p.path,
		aclProvider:  p.aclProvider,
		makeLastNode: false,
	}
}

func (p *ensurePath) Ensure(client *CuratorZookeeperClient) error {
	if helper := p.helper.Load(); helper != nil {
		return helper.(EnsurePathHelper).Ensure(client, p.path, p.makeLastNode)
	}

	return nil
}
