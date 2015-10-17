package main

import (
	"errors"
)

var (
	// needle
	ErrNeedleAlreadyExists   = errors.New("needle already exists")
	ErrNeedleNotExists       = errors.New("needle not exists")
	ErrNeedleChecksum        = errors.New("needle checksum error")
	ErrNeedleFlag            = errors.New("needle flag error")
	ErrNeedleSize            = errors.New("needle size error")
	ErrNeedleHeaderMagic     = errors.New("needle header magic number error")
	ErrNeedleFooterMagic     = errors.New("needle footer magic number error")
	ErrNeedleKeyNotMatch     = errors.New("needle key not match")
	ErrNeedlePaddingNotMatch = errors.New("needle padding not match")
	ErrNeedleCookieNotMatch  = errors.New("needle cookie not match")
	ErrNeedleDeleted         = errors.New("needle deleted")
	// ring
	ErrRingEmpty = errors.New("ring buffer empty")
	ErrRingFull  = errors.New("ring buffer full")
	// index
	ErrIndexOffset = errors.New("index offset error")
	ErrIndexSize   = errors.New("index size error")
	// store
	ErrStoreVolumeIndex = errors.New("store volume index error")
)
