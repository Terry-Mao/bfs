package genkey

log "github.com/golang/glog"

const (
	maxSize        = 1000
	errorSleep     = 100 * time.Millisecond
)

// Genkey generate key for upload file
type Genkey struct {
	client    *Client
	keys      chan int64
}

// NewGenkey 
func NewGenkey(zservers []string, zpath string, ztimeout time.Duration, workerId int64) (g *Genkey, err error) {
	if err = Init(zservers, zpath, ztimeout); err != nil {
		log.Errorf("NewGenkey Init error(%v)", err)
		return nil, err
	}
	g = &Genkey{}
	g.client = NewClient(workerId)
	g.keys = make(chan int64, maxSize)
	go g.preGenerate()
	return
}

// Getkey get key for upload file
func (g *Genkey) Getkey() int64 {
	return <-g.keys
}

// preGenerate pre generate key until 1000
func (g *Genkey) preGenerate() {
	var (
		key   int64
		keys  []int64
		err   error
	)
	for {
		if keys, err = g.client.Ids(100); err != nil {
			log.Errorf("preGenerate() error(%v)  need check!!", err)
			time.Sleep(errorSleep)
			continue
		}
		for _, key := range keys {
			g.keys <- key
		}
	}
}