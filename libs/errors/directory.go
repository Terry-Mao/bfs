package errors

const (
	// hbase
	RetHbaseFailed      = 30100
	// id
	RetNoAvailableId   = 30200
	// store
	RetNoAvailableStore = 30300
	// zookeeper 
	RetZookeeperDataError = 30400
)

var (
	// hbase
	ErrHbaseFailed      = Error(RetHbaseFailed)
	// id
	ErrNoAvailableId        = Error(RetNoAvailableId)
	// store
	ErrNoAvailableStore    = Error(RetNoAvailableStore)
	// zookeeper
	ErrZookeeperDataError  = Error(RetZookeeperDataError)
)
