package main

const (
	// ok
	RetOK = 1
	// api
	RetNoVolume      = 1000
	RetUploadErr     = 1001
	RetUploadMaxFile = 1002
	RetDelErr        = 1003
	RetDelMaxFile    = 1004
	// admin
	RetBulkErr      = 2000
	RetCompactErr   = 2001
	RetAddVolumeErr = 2002
	// err
	RetParamErr    = 65534
	RetInternalErr = 65535
)
