package curator

import (
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

type Watcher func(event *zk.Event)

type Watchers struct {
	lock     sync.Mutex
	watchers []Watcher
}

func NewWatchers(watchers ...Watcher) *Watchers {
	return &Watchers{watchers: watchers}
}

func (w *Watchers) Add(watcher Watcher) Watcher {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.watchers = append(w.watchers, watcher)

	return watcher
}

func (w *Watchers) Remove(watcher Watcher) Watcher {
	w.lock.Lock()
	defer w.lock.Unlock()

	for i, v := range w.watchers {
		if &v == &watcher {
			copy(w.watchers[i:], w.watchers[i+1:])

			return watcher
		}
	}

	return nil
}

func (w *Watchers) Watch(events <-chan zk.Event) {
	for {
		if event, ok := <-events; !ok {
			break
		} else {
			for _, watcher := range w.watchers {
				go watcher(&event)
			}
		}
	}
}
