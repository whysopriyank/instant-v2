package storageapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// signPayload computes hex(HMAC-SHA256(secret, op|appID|id|filename|exp)).
func signPayload(secret []byte, op, appID, id, filename string, exp int64) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(op + "|" + appID + "|" + id + "|" + filename + "|" + strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifySignature checks the expires/signature query params against
// op/appID/id and enforces non-expiry. Any failure is a 403-class error.
func verifySignature(secret []byte, op, appID, id string, q url.Values) error {
	exp, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	if err != nil || exp < 1 {
		return errors.New("storageapi: missing or malformed expiry")
	}
	if !hmac.Equal([]byte(signPayload(secret, op, appID, id, q.Get("filename"), exp)), []byte(q.Get("signature"))) {
		return errors.New("storageapi: invalid signature")
	}
	if time.Now().Unix() > exp {
		return errors.New("storageapi: signature expired")
	}
	return nil
}

// newFileID mints a random RFC-4122 version-4 uuid string.
func newFileID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return platform.UUIDToStr(b)
}
