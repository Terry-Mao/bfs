package needle

const (
	_cacheOffsetBit = 32
	// del offset
	CacheDelOffset = uint32(0)
)

// NeedleCache needle meta data in memory.
// high 32bit = Offset
// low 32 bit = Size
//  ----------------
// |      int64     |
//  ----------------
// | 32bit  | 32bit |
// | offset | size  |
//  ----------------

// NewCache new a needle cache.
func NewCache(offset uint32, size int32) int64 {
	return int64(offset)<<_cacheOffsetBit + int64(size)
}

// Cache get needle cache data.
// return offset, size
func Cache(n int64) (uint32, int32) {
	return uint32(n >> _cacheOffsetBit), int32(n)
}
