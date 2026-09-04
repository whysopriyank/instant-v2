package adminapi

// SetFaultBeforeTokenCommitForTest installs a deterministic failure seam for
// external-package tests. This file is compiled only by `go test`, so the
// production package has no exported failure-injection API or runtime hook.
func SetFaultBeforeTokenCommitForTest(h *Handler, fn func() error) {
	h.faultBeforeTokenCommit = fn
}
