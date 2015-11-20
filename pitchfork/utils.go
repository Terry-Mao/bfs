package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sort"
	"errors"
)


// Divides a set of stores between a set of pitchforks.
func divideStoreBetweenPitchfork(pitchforks PitchforkList, stores StoreList) (map[string]StoreList, error) {
	result := make(map[string]StoreList)

	slen := len(stores)
	plen := len(pitchforks)
	if slen == 0 || plen == 0 || slen < plen {
		return nil, errors.New("divideStoreBetweenPitchfork error")
	}

	sort.Sort(stores)
	sort.Sort(pitchforks)

	n := slen / plen
	m := slen % plen
	p := 0
	for i, pitchfork := range pitchforks {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > slen {
			last = slen
		}

		for _, store := range stores[first:last] {
			result[pitchfork.ID] = append(result[pitchfork.ID], store)
		}
		p = last
	}

	return result, nil
}


func generateUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func generateID() (string, error) {
	var (
		uuid      string
		hostname string
		ID       string
		err      error
	)

	uuid, err = generateUUID()
	if err != nil {
		return "", err
	}

	hostname, err = os.Hostname()
	if err != nil {
		return "", err
	}

	ID = fmt.Sprintf("%s:%s", hostname, uuid)
	return ID, nil
}
