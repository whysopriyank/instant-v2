# QH-001 qualification image: pinned toolchain that can run the full
# `make test-release` gate. Source is always mounted at /src (reproducible
# build path); the evidence dir is mounted read-only outside /src.
FROM golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437

ENV GOTOOLCHAIN=local
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    jq make git procps postgresql-client postgresql ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# F2 (QH-001 R1): the container runs as root over a host-uid checkout, so Go
# VCS stamping shells out to git and fails with "dubious ownership". Mark all
# checkouts safe at the system level. Do NOT use -buildvcs=false: the FR-002
# gate runs plain `make build` in this image and the candidate bytes must
# match that build exactly.
RUN git config --system --add safe.directory '*'

# golangci-lint v2.13.1 via the exact curl + sha256sum -c install used in
# .github/workflows/ci.yml (pinned, hash-verified).
RUN curl -fsSLo golangci-lint.tar.gz https://github.com/golangci/golangci-lint/releases/download/v2.13.1/golangci-lint-2.13.1-linux-amd64.tar.gz && printf 'b17bfbc9d4aaa48be7f4f1ce3240bc3d8200c870c072bacf15c26219e2cfb9cc  golangci-lint.tar.gz\n' | sha256sum -c - \
  && tar -xzf golangci-lint.tar.gz \
  && install -m 0755 golangci-lint-2.13.1-linux-amd64/golangci-lint /usr/local/bin/golangci-lint \
  && rm -rf golangci-lint.tar.gz golangci-lint-2.13.1-linux-amd64

# ripgrep 15.2.0 via the exact pinned install in .github/workflows/ci.yml
# (the test-integration fixture audit requires rg).
RUN curl -fsSLo ripgrep.tar.gz https://github.com/BurntSushi/ripgrep/releases/download/15.2.0/ripgrep-15.2.0-x86_64-unknown-linux-musl.tar.gz && printf '33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c  ripgrep.tar.gz\n' | sha256sum -c - \
  && tar -xzf ripgrep.tar.gz \
  && install -m 0755 ripgrep-15.2.0-x86_64-unknown-linux-musl/rg /usr/local/bin/rg \
  && rm -rf ripgrep.tar.gz ripgrep-15.2.0-x86_64-unknown-linux-musl

# Lanes run as the invoking host user (campaign.sh/gate.sh pass --user):
# several suites assert permission denials that root bypasses, and CI runs
# as a non-root user. Caches live under /tmp so any uid can write them;
# GOMODCACHE is fixed because untrimmed binaries embed module paths.
# initdb/pg_ctl on PATH: cmd/chaos tests need a throwaway cluster, and the
# gate's owned-DB lane rejects any skipped test.
ENV PATH=/usr/lib/postgresql/15/bin:$PATH
ENV HOME=/tmp/home GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomod GOLANGCI_LINT_CACHE=/tmp/golangci
WORKDIR /src
