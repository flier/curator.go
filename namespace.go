package curator

import (
	"errors"
	"strings"
	"sync"
)

type namespaceFacade struct {
	*curatorFramework
	namespace string
}

func (f *namespaceFacade) Start() error {
	return errors.New("the requested operation is not supported")
}

func (f *namespaceFacade) Close() error {
	return errors.New("the requested operation is not supported")
}

func (f *namespaceFacade) CuratorListenable() CuratorListenable {
	panic("CuratorListenable() is only available from a non-namespaced CuratorFramework instance")
}

func (f *namespaceFacade) Namespace() string {
	return f.namespace
}

func (f *namespaceFacade) fixForNamespace(path string, isSequential bool) string {
	return fixForNamespace(f.namespace, path, isSequential)
}

func (f *namespaceFacade) unfixForNamespace(path string) string {
	return unfixForNamespace(f.namespace, path)
}

func fixForNamespace(namespace, path string, isSequential bool) string {
	if len(namespace) > 0 {
		return JoinPath(namespace, path)
	}

	return path
}

func unfixForNamespace(namespace, path string) string {
	if len(namespace) > 0 {
		prefix := JoinPath(namespace)

		if strings.HasPrefix(path, prefix) {
			if len(path) > len(prefix) {
				return path[len(prefix):]
			} else {
				return PATH_SEPARATOR
			}
		}
	}
	return path
}

type namespaceFacadeCache struct {
	client *curatorFramework
	cache  map[string]*namespaceFacade
	lock   sync.Mutex
}

func NewNamespaceFacadeCache(client *curatorFramework) *namespaceFacadeCache {
	return &namespaceFacadeCache{
		client: client,
		cache:  make(map[string]*namespaceFacade),
	}
}

func (c *namespaceFacadeCache) Get(newNamespace string) *namespaceFacade {
	c.lock.Lock()
	defer c.lock.Unlock()

	if facade, exists := c.cache[newNamespace]; exists {
		return facade
	}

	facade := &namespaceFacade{c.client, newNamespace}

	c.cache[newNamespace] = facade

	return facade
}
