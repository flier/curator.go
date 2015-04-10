package curator

type EnsembleProvider interface {
	Start() error

	Close() error

	ConnectionString() string
}

type fixedEnsembleProvider struct {
	connectString string
}

func (p *fixedEnsembleProvider) Start() error { return nil }

func (p *fixedEnsembleProvider) Close() error { return nil }

func (p *fixedEnsembleProvider) ConnectionString() string { return p.connectString }
