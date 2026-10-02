package backup_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

func TestV1RestoreArchiveBoundsAndCRC(t *testing.T) {
	for _, problem := range []string{"compressed-size", "expanded-size", "crc", "file-path", "multiple-stores"} {
		t.Run(problem, func(t *testing.T) {
			archive := restoreZip(t, "config.json", `{"schema":{"blobs":{"todo":{"text":{"valueType":"string"}}}}}`)
			size := int64(len(archive))
			if problem == "compressed-size" {
				size = 1<<30 + 1
			}
			if problem == "expanded-size" || problem == "crc" {
				i := bytes.Index(archive, []byte{'P', 'K', 1, 2})
				if i < 0 {
					t.Fatal("fixture has no ZIP central header")
				}
				if problem == "expanded-size" {
					binary.LittleEndian.PutUint32(archive[i+24:], 1<<30+1)
				} else {
					archive[i+16] ^= 1
				}
			}
			if problem == "file-path" {
				archive = restoreZip(t, "config.json", "{}", "files/../outside", "payload")
				size = int64(len(archive))
			}
			var err error
			if problem == "multiple-stores" {
				_, err = backup.RestoreV1Zip(context.Background(), nil, bytes.NewReader(archive), size, [16]byte{}, nil, nil)
			} else {
				_, err = backup.RestoreV1Zip(context.Background(), nil, bytes.NewReader(archive), size, [16]byte{})
			}
			if err == nil {
				t.Fatal("invalid archive accepted")
			}
			want := map[string]string{"compressed-size": "maximum size", "expanded-size": "maximum size", "crc": "checksum", "file-path": "UUID", "multiple-stores": "at most one"}[problem]
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error=%v, want %q", err, want)
			}
		})
	}
}
