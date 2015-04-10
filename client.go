package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

type CuratorZookeeperClient struct {
	state ZookeeperConnectionState
}

func NewClient() *CuratorZookeeperClient {
	return &CuratorZookeeperClient{}
}

func (c *CuratorZookeeperClient) Start() error {
	return nil
}

func (c *CuratorZookeeperClient) Close() error {
	return nil
}

type ZookeeperConnectionState struct {
	conn *zk.Conn
}
