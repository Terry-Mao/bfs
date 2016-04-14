package auth

import (
	"bfs/libs/errors"
	"bfs/proxy/conf"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	log "github.com/golang/glog"
	"hash"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// bucket acl
	_statusReadBit  = 0
	_statusWriteBit = 1
	//
	_statusRead  = 1 << _statusReadBit
	_statusWrite = 1 << _statusWriteBit
	//
	_authExpire = 900                // s
	_template   = "%s\n%s\n%s\n%d\n" // method bucket filename expire
)

type Auth struct {
	b map[string]Bucket
	c *conf.Config
}

// NewAuth
func NewAuth(c *conf.Config) (a *Auth, err error) {
	a = &Auth{}
	a.c = c
	a.b, err = InitBucket()
	return
}

// CheckAuth
func (a *Auth) CheckAuth(r *http.Request) (err error) {
	var (
		params   = r.URL.Query()
		ss       []string
		bucket   string
		filename string
		token    string
		exist    bool
	)
	if r.Method == "PUT" || r.Method == "DELETE" {
		ss = strings.Split(r.URL.Path[1:], "/")
	} else {
		ss = strings.Split(strings.TrimPrefix(r.URL.Path, "/bfs")[1:], "/")
	}
	bucket = ss[0]
	if _, exist = a.b[bucket]; !exist {
		err = errors.ErrBucketNotExist
		return
	}
	if !a.bucketNeedAuth(r.Method, bucket) {
		return
	}
	filename = ss[len(ss)-1]
	token = params.Get("token")
	if token == "" {
		token = r.Header.Get("Authorization")
	}
	if !a.reqAuth(r.Method, bucket, filename, token) {
		log.Errorf("CheckAuth failed method: %s, bucket: %s, token: %s", r.Method, bucket, token)
		err = errors.ErrAuthFailed
	}
	return
}

// Bucket need check authorization
func (a *Auth) bucketNeedAuth(method, bucket string) bool {
	var (
		property int
	)
	property = a.b[bucket].Property
	if method == "GET" || method == "HEAD" {
		if property&_statusRead == 0 {
			return false
		}
	} else { // POST  DELETE
		if property&_statusWrite == 0 {
			return false
		}
	}
	return true
}

// auth
func (a *Auth) reqAuth(method, bucket, filename, token string) bool {
	var (
		err       error
		now       int64
		keyId     string
		keySecret string
		expire    int64
		auth      string
		realAuth  string
		ss        []string
	)
	ss = strings.Split(token, ":")
	if len(ss) != 3 {
		return false
	}
	keyId = ss[0]
	if keyId != a.b[bucket].KeyId {
		return false
	}
	keySecret = a.b[bucket].KeySecret
	auth = ss[1]
	if expire, err = strconv.ParseInt(ss[2], 10, 64); err != nil {
		return false
	}
	now = time.Now().Unix()
	// > ±15 min is forbidden
	if expire > now {
		if expire-now > _authExpire {
			return false
		}
	} else {
		if now-expire > _authExpire {
			return false
		}
	}
	realAuth = a.createAuthorization(method, bucket, filename, expire, keySecret)
	if auth != realAuth {
		log.Errorf("auth failed: auth: %s   realAuth: %s ", auth, realAuth)
		return false
	}
	return true
}

// createAuthorization
func (a *Auth) createAuthorization(method, bucket, filename string, expire int64, keySecret string) (auth string) {
	var (
		content string
		mac     hash.Hash
	)
	content = fmt.Sprintf(_template, method, bucket, filename, expire)
	mac = hmac.New(sha1.New, []byte(keySecret))
	mac.Write([]byte(content))
	auth = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return
}
