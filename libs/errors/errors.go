package errors

type Error int

func (e Error) Error() string {
	return errorMsg[int(e)]
}

var (
	errorMsg = map[int]string{
		/* ========================= Store ========================= */
		// common
		RetOK:          "ok",
		RetParamErr:    "store param error",
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
		RetSuperBlockOffset:     "super block offset not consistency with size",
		// index
		RetIndexSize:   "index size error",
		RetIndexClosed: "index closed",
		RetIndexOffset: "index offset",
		RetIndexEOF:    "index eof",
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
		RetNeedleHeaderSize:  "needle header size",
		RetNeedleDataSize:    "needle data size",
		RetNeedleFooterSize:  "needle footer size",
		RetNeedlePaddingSize: "needle padding size",
		RetNeedleFull:        "needle full",
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
		/* ========================= Directory ========================= */
		// hbase
		RetHBase: "hbase failed",
		// id
		RetIdNotAvailable: "generate id failed",
		// store
		RetStoreNotAvailable: "store not available",
		// zookeeper
		RetZookeeperDataError: "zookeeper data error",
		/* ========================= Directory ========================= */
		/* ========================= Proxy ========================= */
		// common
		RetBucketNotExist: "bucket not exist",
		RetAuthFailed:     "authorization failed",
		RetUrlBad:         "bad url",
		// upload
		RetFileTooLarge: "file too large",
		/* ========================= Proxy ========================= */
	}
)
