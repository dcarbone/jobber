package jobber

type Job interface {
	RespondTo() chan error
	Process() error
}
