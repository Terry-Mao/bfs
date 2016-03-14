package meta

const (
	blockLeftSpace = uint32(1024 * 1024 * 1) // 1 mb
)

type SuperBlock struct {
	File    string `json:"file"`
	Offset  uint32 `json:"offset"`
	LastErr error  `json:"last_err"`
	Ver     byte   `json:"ver"`
	Padding uint32 `json:"padding"`
}

// Full check the block full.
func (b *SuperBlock) Full() bool {
	return ((MaxBlockOffset - b.Offset) < (blockLeftSpace / b.Padding))
}

// FreeSpace cal rest space of volume
func (b *SuperBlock) FreeSpace() uint32 {
	return MaxBlockOffset - b.Offset
}
