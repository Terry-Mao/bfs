package errors

const (
	// api
	RetUploadMaxFile = 2000
	RetDelMaxFile    = 2001
	// block
	RetSuperBlockMagic      = 3000
	RetSuperBlockVer        = 3001
	RetSuperBlockPadding    = 3002
	RetSuperBlockNoSpace    = 3003
	RetSuperBlockRepairSize = 3004
	RetSuperBlockClosed     = 3005
	RetSuperBlockOffset     = 3006
	// index
	RetIndexSize   = 4000
	RetIndexClosed = 4001
	RetIndexOffset = 4002
	RetIndexEOF    = 4003
	// needle
	RetNeedleNotExist    = 5001
	RetNeedleChecksum    = 5002
	RetNeedleFlag        = 5003
	RetNeedleSize        = 5004
	RetNeedleHeaderMagic = 5005
	RetNeedleFooterMagic = 5006
	RetNeedleKey         = 5007
	RetNeedlePadding     = 5008
	RetNeedleCookie      = 5009
	RetNeedleDeleted     = 5010
	RetNeedleTooLarge    = 5011
	RetNeedleHeaderSize  = 5012
	RetNeedleDataSize    = 5013
	RetNeedleFooterSize  = 5014
	RetNeedlePaddingSize = 5015
	RetNeedleFull        = 5016
	// ring
	RetRingEmpty = 6000
	RetRingFull  = 6001
	// store
	RetStoreVolumeIndex  = 7000
	RetStoreNoFreeVolume = 7001
	RetStoreFileExist    = 7002
	// volume
	RetVolumeExist     = 8000
	RetVolumeNotExist  = 8001
	RetVolumeDel       = 8002
	RetVolumeInCompact = 8003
	RetVolumeClosed    = 8004
	RetVolumeBatch     = 8005
)

var (
	ErrUploadMaxFile = Error(RetUploadMaxFile)
	ErrDelMaxFile    = Error(RetDelMaxFile)
	// block
	ErrSuperBlockMagic      = Error(RetSuperBlockMagic)
	ErrSuperBlockVer        = Error(RetSuperBlockVer)
	ErrSuperBlockPadding    = Error(RetSuperBlockPadding)
	ErrSuperBlockNoSpace    = Error(RetSuperBlockNoSpace)
	ErrSuperBlockRepairSize = Error(RetSuperBlockRepairSize)
	ErrSuperBlockClosed     = Error(RetSuperBlockClosed)
	ErrSuperBlockOffset     = Error(RetSuperBlockOffset)
	// index
	ErrIndexSize   = Error(RetIndexSize)
	ErrIndexClosed = Error(RetIndexClosed)
	ErrIndexOffset = Error(RetIndexOffset)
	ErrIndexEOF    = Error(RetIndexEOF)
	// needle
	ErrNeedleNotExist    = Error(RetNeedleNotExist)
	ErrNeedleChecksum    = Error(RetNeedleChecksum)
	ErrNeedleFlag        = Error(RetNeedleFlag)
	ErrNeedleSize        = Error(RetNeedleSize)
	ErrNeedleHeaderMagic = Error(RetNeedleHeaderMagic)
	ErrNeedleFooterMagic = Error(RetNeedleFooterMagic)
	ErrNeedleKey         = Error(RetNeedleKey)
	ErrNeedlePadding     = Error(RetNeedlePadding)
	ErrNeedleCookie      = Error(RetNeedleCookie)
	ErrNeedleDeleted     = Error(RetNeedleDeleted)
	ErrNeedleTooLarge    = Error(RetNeedleTooLarge)
	ErrNeedleHeaderSize  = Error(RetNeedleHeaderSize)
	ErrNeedleDataSize    = Error(RetNeedleDataSize)
	ErrNeedleFooterSize  = Error(RetNeedleFooterSize)
	ErrNeedlePaddingSize = Error(RetNeedlePaddingSize)
	ErrNeedleFull        = Error(RetNeedleFull)
	// ring
	ErrRingEmpty = Error(RetRingEmpty)
	ErrRingFull  = Error(RetRingFull)
	// store
	ErrStoreVolumeIndex  = Error(RetStoreVolumeIndex)
	ErrStoreNoFreeVolume = Error(RetStoreNoFreeVolume)
	ErrStoreFileExist    = Error(RetStoreFileExist)
	// volume
	ErrVolumeExist     = Error(RetVolumeExist)
	ErrVolumeNotExist  = Error(RetVolumeNotExist)
	ErrVolumeDel       = Error(RetVolumeDel)
	ErrVolumeInCompact = Error(RetVolumeInCompact)
	ErrVolumeClosed    = Error(RetVolumeClosed)
	ErrVolumeBatch     = Error(RetVolumeBatch)
)
