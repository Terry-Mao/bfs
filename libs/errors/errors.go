package errors

type Error int

func (e Error) Error() string {
	return errorMsg[int(e)]
}

var (
	errorMsg = map[int]string{
		/* ========================= Store ========================= */
		// common
		RetOK:       "ok",
		RetParamErr: "store param error",
		RetInternalErr: "internal server error",
		// api
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
		RetStoreNoFreeVolume: "store no free volume",
		RetStoreFileExist:    "store rename file exist",
		// volume
		RetVolumeExist:     "volume exist",
		RetVolumeNotExist:  "volume not exist",
		RetVolumeDel:       "volume deleted",
		RetVolumeInCompact: "volume in compacting",
		RetVolumeClosed:    "volume closed",
		RetVolumeBatch:     "volume exceed batch write number",
		/* ========================= Store ========================= */
	}
)
