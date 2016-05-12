package global
import "bfs/gops/models/types"

var (
	MAX_GROUP_ID uint64 = 0;
	MAX_VOLUME_ID uint64 = 0;
	STORES  map[string]*types.Store
	IN_GROUP_STORES  map[string]*types.Store
	GROUPS map[uint64]*types.Group
)
