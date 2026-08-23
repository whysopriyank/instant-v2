package sync

import (
	"crypto/rand"
	"fmt"
	"sync"
)

func newID(counter *sync.Mutex) string {
	counter.Lock()
	defer counter.Unlock()
	var b [8]byte
	rand.Read(b[:])
	return fmt.Sprintf("sess-%x", b[:])
}

func parseUUIDOrZero(s string) [16]byte {
	var u [16]byte
	hexOnly := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '-' {
			continue
		}
		v, ok := hexValByte(c)
		if !ok {
			return u
		}
		hexOnly = append(hexOnly, v)
	}
	if len(hexOnly) != 32 {
		return u
	}
	for i := range 16 {
		u[i] = hexOnly[2*i]<<4 | hexOnly[2*i+1]
	}
	return u
}

func hexValByte(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
