package storageapi

import (
	"io"
	"time"
)

// S3Config carries everything a real S3 backend would need. It exists so the
// constructor surface is already final for cmd/ wiring even though the SDK is
// not wired yet.
type S3Config struct {
	Bucket     string
	Region     string
	Endpoint   string // optional; for MinIO-style path-style deployments
	AccessKey  string
	SecretKey  string
	PresignTTL time.Duration
}

// S3Backend is the ObjectStore implementation for real S3 deployments,
// mirroring v1 instant.storage.s3 (generate-presigned-url-put /
// create-signed-download-url!, upload-file-to-s3, bulk-delete-files!).
//
// DEPENDENCY NOTE for the orchestrator: implementing this requires
// github.com/aws/aws-sdk-go-v2 (+ its config and service/s3 modules), which
// are NOT in go.mod and this package may not edit go.mod. Every method
// therefore returns ErrNotConfigured until that dependency lands; wire
// DiskBackend in the meantime.
type S3Backend struct {
	Config S3Config
	// secret will sign our own passthrough URLs if the passthrough mode is
	// kept once implemented.
	secret []byte
}

var _ ObjectStore = (*S3Backend)(nil)

func NewS3Backend(cfg S3Config, secret []byte) *S3Backend {
	return &S3Backend{Config: cfg, secret: secret}
}

func (*S3Backend) PresignUpload(string, time.Duration) (string, error) {
	return "", ErrNotConfigured
}

func (*S3Backend) PresignDownload(string, time.Duration) (string, error) {
	return "", ErrNotConfigured
}

func (*S3Backend) Put(string, io.Reader) error { return ErrNotConfigured }

func (*S3Backend) Open(string) (*Object, error) { return nil, ErrNotConfigured }

func (*S3Backend) Delete([]string) error { return ErrNotConfigured }
