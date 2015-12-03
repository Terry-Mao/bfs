package meta

var (
	HbaseTable = []byte("bfsmeta")

	HbaseFamilyBasic   = []byte("basic")

	HbaseColumnVid     = []byte("vid")
	HbaseColumnCookie  = []byte("cookie")
)

type Meta struct {
	// click
	Key     int64 `json:"key"`
	Vid     int32 `json:"vid"`
	Cookie  int32 `json:"cookie"`
//status update_time ......
}