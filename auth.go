package curator

type AuthInfo interface {
	Scheme() string

	Auth() []byte
}

type authInfo struct {
	scheme string
	auth   []byte
}

func (a *authInfo) Scheme() string { return a.scheme }

func (a *authInfo) Auth() []byte { return a.auth }
