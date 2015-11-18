package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sort"
)


// Divides a set of stores between a set of pitchforks.
func divideStoreBetweenPitchfork(pitchforks PitchforkList, stores StoreList) map[string]StoreList {
	var result StoreList

	slen := len(stores)
	plen := len(pitchforks)
	if clen == 0 {
		return result
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
		if last > plen {
			last = plen
		}

		for _, store := range stores[first:last] {
			result[pitchfork.ID] = append(result[pitchfork.ID], store)
		}
		p = last
	}

	return result
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

func generateID() (ID string, err error) {
	var uuid, hostname string

	uuid, err = generateUUID()
	if err != nil {
		return
	}

	hostname, err = os.Hostname()
	if err != nil {
		return
	}

	ID = fmt.Sprintf("%s:%s", hostname, uuid)
	return
}
