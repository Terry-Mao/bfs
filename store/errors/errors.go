package errors

const (
	// common
	RetOK            = 1
	RetUploadMaxFile = 2
	RetDelMaxFile    = 3
	RetParamErr      = 65534
	RetInternalErr   = 65535
	// api
	// block
	RetSuperBlockMagic      = 3000
	RetSuperBlockVer        = 3001
	RetSuperBlockPadding    = 3002
	RetSuperBlockNoSpace    = 3003
	RetSuperBlockRepairSize = 3004
	RetSuperBlockClosed     = 3005
	// index
	RetIndexSize   = 4000
	RetIndexClosed = 4001
	// needle
	RetNeedleExist       = 5000
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
	// ring
	RetRingEmpty = 6000
	RetRingFull  = 6001
	// store
	RetStoreVolumeIndex  = 7000
	RetStoreNoFreeVolume = 7001
	// volume
	RetVolumeExist     = 8000
	RetVolumeNotExist  = 8001
	RetVolumeDel       = 8002
	RetVolumeInCompact = 8003
	RetVolumeClosed    = 8004
	RetVolumeBatch     = 8005
)

type StoreError int

func (e StoreError) Error() string {
	return storeErrorMsg[int(e)]
}

var (
	// common
	ErrParam         = StoreError(RetParamErr)
	ErrUploadMaxFile = StoreError(RetUploadMaxFile)
	// block
	ErrSuperBlockMagic      = StoreError(RetSuperBlockMagic)
	ErrSuperBlockVer        = StoreError(RetSuperBlockVer)
	ErrSuperBlockPadding    = StoreError(RetSuperBlockPadding)
	ErrSuperBlockNoSpace    = StoreError(RetSuperBlockNoSpace)
	ErrSuperBlockRepairSize = StoreError(RetSuperBlockRepairSize)
	ErrSuperBlockClosed     = StoreError(RetSuperBlockClosed)
	// index
	ErrIndexSize   = StoreError(RetIndexSize)
	ErrIndexClosed = StoreError(RetIndexClosed)
	// needle
	ErrNeedleExist       = StoreError(RetNeedleExist)
	ErrNeedleNotExist    = StoreError(RetNeedleNotExist)
	ErrNeedleChecksum    = StoreError(RetNeedleChecksum)
	ErrNeedleFlag        = StoreError(RetNeedleFlag)
	ErrNeedleSize        = StoreError(RetNeedleSize)
	ErrNeedleHeaderMagic = StoreError(RetNeedleHeaderMagic)
	ErrNeedleFooterMagic = StoreError(RetNeedleFooterMagic)
	ErrNeedleKey         = StoreError(RetNeedleKey)
	ErrNeedlePadding     = StoreError(RetNeedlePadding)
	ErrNeedleCookie      = StoreError(RetNeedleCookie)
	ErrNeedleDeleted     = StoreError(RetNeedleDeleted)
	ErrNeedleTooLarge    = StoreError(RetNeedleTooLarge)
	// ring
	ErrRingEmpty = StoreError(RetRingEmpty)
	ErrRingFull  = StoreError(RetRingFull)
	// store
	ErrStoreVolumeIndex  = StoreError(RetStoreVolumeIndex)
	ErrStoreNoFreeVolume = StoreError(RetStoreNoFreeVolume)
	// volume
	ErrVolumeExist     = StoreError(RetVolumeExist)
	ErrVolumeNotExist  = StoreError(RetVolumeNotExist)
	ErrVolumeDel       = StoreError(RetVolumeDel)
	ErrVolumeInCompact = StoreError(RetVolumeInCompact)
	ErrVolumeClosed    = StoreError(RetVolumeClosed)
	ErrVolumeBatch     = StoreError(RetVolumeBatch)
)

var (
	storeErrorMsg = map[int]string{
		// common
		RetOK:            "ok",
		RetParamErr:      "param error",
		RetUploadMaxFile: "exceed upload max file num",
		// block
		RetSuperBlockMagic:      "super block magic not match",
		RetSuperBlockVer:        "super block ver not match",
		RetSuperBlockPadding:    "super block padding not match",
		RetSuperBlockNoSpace:    "super block no left free space",
		RetSuperBlockRepairSize: "super block repair size must equal original",
		RetSuperBlockClosed:     "super block closed",
		// index
		RetIndexSize:   "index size error",
		RetIndexClosed: "index closed",
		// needle
		RetNeedleExist:       "needle already exist",
		RetNeedleNotExist:    "needle not exist",
		RetNeedleChecksum:    "needle data checksum not match",
		RetNeedleFlag:        "needle flag not match",
		RetNeedleSize:        "needle size error",
		RetNeedleHeaderMagic: "needle header magic not match",
		RetNeedleFooterMagic: "needle footer magic not match",
		RetNeedleKey:         "needle key not match",
		RetNeedlePadding:     "needle padding not match",
		RetNeedleCookie:      "needle cookie not match",
		RetNeedleDeleted:     "needle deleted",
		RetNeedleTooLarge:    "needle has no left free space",
		// ring
		RetRingEmpty: "index ring buffer empty",
		RetRingFull:  "index ring buffer full",
		// store
		RetStoreVolumeIndex:  "store volume index",
		RetStoreNoFreeVolume: "",
		// volume
		RetVolumeExist:     "volume exist",
		RetVolumeNotExist:  "volume not exist",
		RetVolumeDel:       "volume deleted",
		RetVolumeInCompact: "volume in compacting",
		RetVolumeClosed:    "volume closed",
		RetVolumeBatch:     "volume exceed batch write number",
	}
)
