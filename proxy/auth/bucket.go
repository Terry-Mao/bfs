package auth

// bucket_name  property  key_id  key_secret
// property   第0位：读 (0表示共有，1表示私有)  第1位：写 (0表示共有，1表示私有)

// todo  ->   db

type Bucket struct {
	BucketName string
	Property   int
	KeyId      string
	KeySecret  string
}

// InitBucket
// todo  get data from db
func InitBucket() (b map[string]Bucket, err error) {
	var (
		item Bucket
	)
	b = make(map[string]Bucket)
	// bucket test
	item.BucketName = "test"
	item.Property = 2
	item.KeyId = "121bce6492eba701"
	item.KeySecret = "1eb80603e85842542f9736eb13b7e1"
	b["test"] = item

	return
}
