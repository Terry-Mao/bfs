package auth

import (
	"bfs/libs/errors"
	ibucket "bfs/proxy/bucket"
	"bfs/proxy/conf"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"
)

const (
	_authExpire = 900                // 15min
	_template   = "%s\n%s\n%s\n%d\n" // method bucket filename expire
)

type Auth struct {
	c *conf.Config
}

// NewAuth
func New(c *conf.Config) (a *Auth, err error) {
	a = &Auth{}
	a.c = c
	return
}

func (a *Auth) Authorize(item *ibucket.Item, method, bucket, file, token string) (err error) {
	// token keyid:sign:time
	var (
		expire int64
		delta  int64
		now    int64
		keyId  string
		ss     = strings.Split(token, ":")
	)
	if len(ss) != 3 {
		return errors.ErrAuthFailed
	}
	keyId = ss[0]
	if keyId != item.KeyId {
		return errors.ErrAuthFailed
	}
	if expire, err = strconv.ParseInt(ss[2], 10, 64); err != nil {
		return errors.ErrAuthFailed
	}
	now = time.Now().Unix()
	// > ±15 min is forbidden
	if expire > now {
		delta = expire - now
	} else {
		delta = now - expire
	}
	if delta > _authExpire {
		return errors.ErrAuthFailed
	}
	err = a.sign(ss[1], method, bucket, file, item.KeySecret, expire)
	return
}

func (a *Auth) sign(src, method, bucket, file, keySecret string, expire int64) (err error) {
	var (
		content string
		mac     hash.Hash
	)
	content = fmt.Sprintf(_template, method, bucket, file, expire)
	mac = hmac.New(sha1.New, []byte(keySecret))
	mac.Write([]byte(content))
	if base64.StdEncoding.EncodeToString(mac.Sum(nil)) != src {
		return errors.ErrAuthFailed
	}
	return
}
