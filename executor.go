package curator

type Runnable func() error

type Executor interface {
	Execute(command Runnable) error
}
