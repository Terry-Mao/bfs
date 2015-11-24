package errors

const (
	RetOK          = 1
	RetParamErr    = 65534
	RetInternalErr = 65535
)

var (
	// common
	ErrParam    = Error(RetParamErr)
	ErrInternal = Error(RetInternalErr)
)
