package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

type CuratorZookeeperClient struct {
	state CuratorFrameworkState
}

func NewClient() *CuratorZookeeperClient {
	return &CuratorZookeeperClient{}
}
