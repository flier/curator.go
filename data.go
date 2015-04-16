package curator

import (
	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

type setDataBuilder struct {
	client        *curatorFramework
	backgrounding backgrounding
	version       int
	compress      bool
}

func (b *setDataBuilder) ForPath(path string) (*zk.Stat, error) {
	return b.ForPathWithData(path, b.client.defaultData)
}

func (b *setDataBuilder) ForPathWithData(givenPath string, payload []byte) (*zk.Stat, error) {
	if b.compress {
		if data, err := b.client.compressionProvider.Compress(givenPath, payload); err != nil {
			return nil, err
		} else {
			payload = data
		}
	}

	adjustedPath := b.client.fixForNamespace(givenPath, false)

	if b.backgrounding.inBackground {
		go b.pathInBackground(adjustedPath, payload, givenPath)

		return nil, nil
	} else {
		return b.pathInForeground(adjustedPath, payload)
	}
}

func (b *setDataBuilder) pathInBackground(path string, payload []byte, givenPath string) {
	tracer := b.client.ZookeeperClient().startTracer("setDataBuilder.pathInBackground")

	defer tracer.Commit()

	stat, err := b.pathInForeground(path, payload)

	event := &curatorEvent{
		eventType: SET_DATA,
		err:       err,
		path:      b.client.unfixForNamespace(path),
		data:      payload,
		stat:      stat,
		context:   b.backgrounding.context,
	}

	if b.backgrounding.callback != nil {
		b.backgrounding.callback(b.client, event)
	} else if glog.V(3) {
		glog.V(3).Infof("ignore CREATE event: %s", event)
	}
}

func (b *setDataBuilder) pathInForeground(path string, payload []byte) (*zk.Stat, error) {
	zkClient := b.client.ZookeeperClient()

	result, err := zkClient.newRetryLoop().CallWithRetry(func() (interface{}, error) {
		if conn, err := zkClient.Conn(); err != nil {
			return nil, err
		} else {
			return conn.Set(path, payload, int32(b.version))
		}
	})

	stat, _ := result.(*zk.Stat)

	return stat, err
}

func (b *setDataBuilder) WithVersion(version int) SetDataBuilder {
	b.version = version

	return b
}

func (b *setDataBuilder) Compressed() SetDataBuilder {
	b.compress = true

	return b
}

func (b *setDataBuilder) InBackground() SetDataBuilder {
	b.backgrounding = backgrounding{inBackground: true}

	return b
}

func (b *setDataBuilder) InBackgroundWithContext(context interface{}) SetDataBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context}

	return b
}

func (b *setDataBuilder) InBackgroundWithCallback(callback BackgroundCallback) SetDataBuilder {
	b.backgrounding = backgrounding{inBackground: true, callback: callback}

	return b
}

func (b *setDataBuilder) InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) SetDataBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context, callback: callback}

	return b
}
