package main

import (
	"errors"
)

var (
	// block
	ErrSuperBlockMagic      = errors.New("super block magic error")
	ErrSuperBlockVer        = errors.New("super block ver error")
	ErrSuperBlockPadding    = errors.New("super block padding error")
	ErrSuperBlockNoSpace    = errors.New("super block no left free space")
	ErrSuperBlockRepairSize = errors.New("super block repair size")
	// index
	ErrIndexSize = errors.New("index size error")
	// needle
	ErrNeedleExists      = errors.New("needle already exists")
	ErrNoNeedle          = errors.New("needle not exists")
	ErrNeedleChecksum    = errors.New("needle checksum error")
	ErrNeedleFlag        = errors.New("needle flag error")
	ErrNeedleSize        = errors.New("needle size error")
	ErrNeedleHeaderMagic = errors.New("needle header magic number error")
	ErrNeedleFooterMagic = errors.New("needle footer magic number error")
	ErrNeedleKey         = errors.New("needle key not match")
	ErrNeedlePadding     = errors.New("needle padding error")
	ErrNeedleCookie      = errors.New("needle cookie error")
	ErrNeedleDeleted     = errors.New("needle deleted")
	ErrNeedleTooLarge    = errors.New("needle too large")
	// ring
	ErrRingEmpty = errors.New("ring buffer empty")
	ErrRingFull  = errors.New("ring buffer full")
	// store
	ErrStoreVolumeIndex  = errors.New("store volume index error")
	ErrStoreNoFreeVolume = errors.New("store has no free volume")
	// volume
	ErrVolumeNotExist  = errors.New("volume not exist")
	ErrVolumeDel       = errors.New("volume del error, may volume del goroutine crash or io too slow")
	ErrVolumeInCompact = errors.New("volume in compact")
)
