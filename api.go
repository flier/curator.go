package curator

import (
	"github.com/samuel/go-zookeeper/zk"
)

const AnyVersion int32 = -1

type CreateMode int32

const (
	PERSISTENT            CreateMode = 0
	PERSISTENT_SEQUENTIAL            = zk.FlagSequence
	EPHEMERAL                        = zk.FlagEphemeral
	EPHEMERAL_SEQUENTIAL             = zk.FlagEphemeral + zk.FlagSequence
)

func (m CreateMode) IsSequential() bool { return (m & zk.FlagSequence) == zk.FlagSequence }
func (m CreateMode) IsEphemeral() bool  { return (m & zk.FlagEphemeral) == zk.FlagEphemeral }

// Called when the async background operation completes
type BackgroundCallback func(client CuratorFramework, event CuratorEvent) error
