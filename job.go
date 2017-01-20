package dcarbone

type Job interface {
	Name() string
	RespondTo() chan error
	Process() error
}
