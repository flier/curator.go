package curator

type deleteBuilder struct {
	client                   *curatorFramework
	backgrounding            backgrounding
	deletingChildrenIfNeeded bool
	guaranteed               bool
	version                  int
}

func (b *deleteBuilder) ForPath(givenPath string) error {
	adjustedPath := b.client.fixForNamespace(givenPath, false)

	if b.backgrounding.inBackground {
		go b.pathInBackground(adjustedPath, givenPath)

		return nil
	} else {
		return b.pathInForeground(adjustedPath)
	}
}

func (b *deleteBuilder) pathInBackground(path string, givenPath string) {
	tracer := b.client.ZookeeperClient().startTracer("deleteBuilder.pathInBackground")

	defer tracer.Commit()

	err := b.pathInForeground(path)

	if b.backgrounding.callback != nil {
		event := &curatorEvent{
			eventType: DELETE,
			err:       err,
			path:      b.client.unfixForNamespace(path),
			context:   b.backgrounding.context,
		}

		if err != nil {
			event.path = givenPath
		}

		event.name = GetNodeFromPath(event.path)

		b.backgrounding.callback(b.client, event)
	}
}

func (b *deleteBuilder) pathInForeground(path string) error {
	zkClient := b.client.ZookeeperClient()

	_, err := zkClient.newRetryLoop().CallWithRetry(func() (interface{}, error) {
		if conn, err := zkClient.Conn(); err != nil {
			return nil, err
		} else {
			return nil, conn.Delete(path, int32(b.version))
		}
	})

	return err
}

func (b *deleteBuilder) DeletingChildrenIfNeeded() DeleteBuilder {
	b.deletingChildrenIfNeeded = true

	return b
}

func (b *deleteBuilder) Guaranteed() DeleteBuilder {
	b.guaranteed = true

	return b
}

func (b *deleteBuilder) WithVersion(version int) DeleteBuilder {
	b.version = version

	return b
}

func (b *deleteBuilder) InBackground() DeleteBuilder {
	b.backgrounding = backgrounding{inBackground: true}

	return b
}

func (b *deleteBuilder) InBackgroundWithContext(context interface{}) DeleteBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context}

	return b
}

func (b *deleteBuilder) InBackgroundWithCallback(callback BackgroundCallback) DeleteBuilder {
	b.backgrounding = backgrounding{inBackground: true, callback: callback}

	return b
}

func (b *deleteBuilder) InBackgroundWithCallbackAndContext(callback BackgroundCallback, context interface{}) DeleteBuilder {
	b.backgrounding = backgrounding{inBackground: true, context: context, callback: callback}

	return b
}
