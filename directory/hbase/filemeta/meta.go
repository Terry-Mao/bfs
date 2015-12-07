package filemeta

var (
	HbaseTable = []byte("bfsmeta")

	HbaseFamilyBasic   = []byte("basic")

	HbaseColumnVid     = []byte("vid")
	HbaseColumnCookie  = []byte("cookie")
)

// File Hbase
type File struct {
	Key     int64 `json:"key"`
	Vid     int32 `json:"vid"`
	Cookie  int32 `json:"cookie"`
//status update_time ......
}