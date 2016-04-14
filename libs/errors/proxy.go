package errors

const (
	// common
	RetUrlBad         = 400
	RetAuthFailed     = 401
	RetBucketNotExist = 404
	// upload
	RetFileTooLarge = 413
)

var (
	// common
	ErrUrlBad         = Error(RetUrlBad)
	ErrAuthFailed     = Error(RetAuthFailed)
	ErrBucketNotExist = Error(RetBucketNotExist)
	// upload
	ErrFileTooLarge = Error(RetFileTooLarge)
)
