package bucket

import (
	"bfs/libs/errors"
	"fmt"
)

const (
	// status bit
	_privateReadBit  = 0
	_privateWriteBit = 1
	// status
	_public           = int(0)
	_privateRead      = int(1 << _privateReadBit)
	_privateWrite     = int(1 << _privateWriteBit)
	_privateReadWrite = int(_privateRead | _privateWrite)
)

// bucket_name  property  key_id  key_secret
type Bucket struct {
	data map[string]*Item
}

type Item struct {
	Name      string
	KeyId     string
	KeySecret string
	Domain    string
	PurgeCDN  bool

	// property   第0位：读 (0表示共有，1表示私有)  第1位：写 (0表示共有，1表示私有)
	property int
}

func (i *Item) String() string {
	return fmt.Sprintf("{name: %s, purge: %s, property: %d}", i.Name, i.PurgeCDN, i.property)
}

func (i *Item) writePublic() bool {
	return i.property&_privateWrite == 0
}

func (i *Item) readPublic() bool {
	return i.property&_privateRead == 0
}

// Public check the item is public or not.
func (i *Item) Public(read bool) bool {
	if read {
		return i.readPublic()
	}
	return i.writePublic()
}

// New a bucket.
func New() (b *Bucket, err error) {
	var item *Item
	b = new(Bucket)
	b.data = make(map[string]*Item)
	// bucket test
	item = new(Item)
	item.Name = "test"
	item.property = _privateWrite
	item.KeyId = "221bce6492eba70f"
	item.KeySecret = "6eb80603e85842542f9736eb13b7e3"
	item.PurgeCDN = false
	b.data[item.Name] = item
	return
}

// Get get a bucket, if not exist then error.
func (b *Bucket) Get(name string) (item *Item, err error) {
	var ok bool
	if item, ok = b.data[name]; !ok {
		err = errors.ErrBucketNotExist
	}
	return
}
