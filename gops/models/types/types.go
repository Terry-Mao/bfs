package types

type Store struct {
	Id     string `json:"id"`
	Ip     string `json:"ip"`
	Api    string `json:"api"`
	Stat   string `json:"stat"`
	Admin  string `json:"admin"`
	Rack   string `json:"rack"`
	Status int `json:"status"`
	Volumes []string `json:"volumes"`
}

type Rack struct {
	Name   string `json:"name"`
	Stores []*Store `json:"stores"`
}


type Group struct {
	Id uint64 `json:"id"`
	StoreIds []string `json:"storeIds"`
	Stores []*Store `json:"stores"`
}


type Volume struct{
	Id uint64 `json:"id"`
	TotalWriteProcessed uint32 `json:"total_write_processed"`
	TotalWriteDelay uint32 `json:"total_write_processed"`
	FreeSpace uint64 `json:"free_space"`
	StoreIds []string `json:"storeIds"`
}


type JsonResponse struct {
	code int
	data interface{}
}