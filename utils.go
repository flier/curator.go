package curator

import (
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
