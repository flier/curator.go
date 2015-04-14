package curator

type EnsembleProvider interface {
	// Curator will call this method when CuratorZookeeperClient.Start() is called
	Start() error

	// Curator will call this method when CuratorZookeeperClient.Close() is called
	Close() error

	// Return the current connection string to use
	ConnectionString() string
}

type fixedEnsembleProvider struct {
	connectString string
}

func (p *fixedEnsembleProvider) Start() error { return nil }

func (p *fixedEnsembleProvider) Close() error { return nil }

func (p *fixedEnsembleProvider) ConnectionString() string { return p.connectString }
