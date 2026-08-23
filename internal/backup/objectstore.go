package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore is the minimal object-storage surface the backup pipeline
// needs: put a full object from a reader of known size, get it back.
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// S3Config configures an S3-compatible ObjectStore (AWS S3, MinIO,
// Cloudflare R2 with path-style=true, …).
type S3Config struct {
	Endpoint  string // host:port
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	// PathStyle forces path-style addressing (<endpoint>/<bucket>/<key>)
	// required by most non-AWS S3 implementations.
	PathStyle bool
	Region    string
}

// S3Store is an ObjectStore over any S3-compatible API.
type S3Store struct {
	cli    *minio.Client
	bucket string
}

var _ ObjectStore = (*S3Store)(nil)

// NewS3Store validates its bucket up front so misconfiguration surfaces at
// wiring time rather than on first export.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, fmt.Errorf("backup: s3 store requires endpoint and bucket")
	}
	cli, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.UseSSL,
		Region:       cfg.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("backup: s3 client: %w", err)
	}
	ok, err := cli.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("backup: s3 bucket %q probe: %w", cfg.Bucket, err)
	}
	if !ok {
		return nil, fmt.Errorf("backup: s3 bucket %q not found", cfg.Bucket)
	}
	return &S3Store{cli: cli, bucket: cfg.Bucket}, nil
}

// Put uploads one object; size < 0 means unknown (multipart).
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.cli.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/x-ndjson"})
	return err
}

// Get streams one object; the caller must close the reader. A missing key
// maps to an error satisfying errors.Is(err, ErrObjectNotFound).
func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.cli.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapS3Err(err)
	}
	// Force the error out at call time, not first read, when cheap to do so.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, mapS3Err(err)
	}
	return obj, nil
}

// ErrObjectNotFound reports a missing object across backends.
var ErrObjectNotFound = fmt.Errorf("backup: object not found")

func mapS3Err(err error) error {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrObjectNotFound, resp.Key)
	}
	return err
}
