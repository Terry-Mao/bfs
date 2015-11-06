package needle

const (
	cacheOffsetBit = 32
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
	return int64(offset)<<cacheOffsetBit + int64(size)
}

// Cache get needle cache data.
func Cache(n int64) (offset uint32, size int32) {
	offset, size = uint32(n>>cacheOffsetBit), int32(n)
	return
}
