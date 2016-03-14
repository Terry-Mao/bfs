package needle

// NeedleOffset convert offset to needle offset.
func NeedleOffset(offset int64) uint32 {
	return uint32(offset / PaddingSize)
}

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * PaddingSize
}

// align get aligned size.
func align(d int32) int32 {
	return (d + _paddingAlign) & ^_paddingAlign
}

// Size get a needle size with meta data.
func Size(n int) int {
	return int(align(_headerSize + int32(n) + _footerSize))
}
