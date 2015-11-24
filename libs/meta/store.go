package meta

const (
	// bit
	StoreStatusEnableBit = 31
	StoreStatusReadBit   = 0
	StoreStatusWriteBit  = 1
	// status
	StoreStatusInit   = 0
	StoreStatusEnable = (1 << StoreStatusEnableBit)
	StoreStatusRead   = StoreStatusEnable | (1 << StoreStatusReadBit)
	StoreStatusWrite  = StoreStatusEnable | (1 << StoreStatusWriteBit)
	StoreStatusHealth = StoreStatusRead | StoreStatusWrite
)

// store zk meta data.
type Store struct {
	Stat   string `json:"stat"`
	Admin  string `json:"admin"`
	Api    string `json:"api"`
	Id     string `json:"id"`
	Rack   string `json:"rack"`
	Status int    `json:"status"`
}
