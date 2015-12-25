package errors

const (
	RetOK          = 1
	RetParamErr    = 65534
	RetInternalErr = 65535

	// needle
	RetNeedleExist = 5000
)

var (
	// common
	ErrParam    = Error(RetParamErr)
	ErrInternal = Error(RetInternalErr)

	ErrNeedleExist = Error(RetNeedleExist)
)
