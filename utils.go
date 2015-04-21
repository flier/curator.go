package curator

import (
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
