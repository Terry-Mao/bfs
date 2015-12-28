package errors

const (
	// hbase
	RetHbase      = 30100
	// id
	RetIdNotAvailable   = 30200
	// store
	RetStoreNotAvailable = 30300
	// zookeeper 
	RetZookeeperDataError = 30400
)

var (
	// hbase
	ErrHbase      = Error(RetHbase)
	// id
	ErrIdNotAvailable        = Error(RetIdNotAvailable)
	// store
	ErrStoreNotAvailable    = Error(RetStoreNotAvailable)
	// zookeeper
	ErrZookeeperDataError  = Error(RetZookeeperDataError)
)
