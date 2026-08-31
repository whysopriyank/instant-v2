package benchrun

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func CanonicalJSON(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	// Decode numbers as json.Number before re-encoding. The default
	// interface decoder converts integers to float64, which silently aliases
	// adjacent int64 authorization seeds above 2^53.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var x any
	if e = dec.Decode(&x); e != nil {
		return nil, e
	}
	var extra any
	if e = dec.Decode(&extra); e != io.EOF {
		if e == nil {
			return nil, errors.New("canonical JSON contains trailing values")
		}
		return nil, e
	}
	return json.Marshal(x)
}

func DigestJSON(v any) (string, error) {
	b, e := CanonicalJSON(v)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

func hashFileBoundedSize(path string, limit int64) ([32]byte, int64, error) {
	var zero [32]byte
	if limit <= 0 {
		return zero, 0, errors.New("invalid artifact read limit")
	}
	f, size, err := openRegularArtifact(path)
	if err != nil {
		return zero, 0, err
	}
	defer f.Close()
	if size > limit {
		return zero, size, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	h := sha256.New()
	n, err := io.CopyN(h, f, limit+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return zero, size, err
	}
	if n > limit {
		return zero, n, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	if n != size {
		return zero, n, errors.New("artifact changed while hashing")
	}
	current, err := f.Stat()
	if err != nil {
		return zero, n, err
	}
	if current.Size() != size {
		return zero, current.Size(), errors.New("artifact changed while hashing")
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, n, nil
}

func hashFileBounded(path string, limit int64) ([32]byte, error) {
	sum, _, err := hashFileBoundedSize(path, limit)
	return sum, err
}

func hashFileStringBounded(path string, limit int64) (string, error) {
	sum, err := hashFileBounded(path, limit)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sum[:]), nil
}

func isSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
