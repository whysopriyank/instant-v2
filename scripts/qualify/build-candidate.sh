#!/usr/bin/env bash
# Reproducible candidate build shared by public qualification and the gate.
set -euo pipefail
profile=${1:-single-node-alpha}
out=${2:-bin/instantd}
case "$profile" in
  single-node-alpha) make build;;
  single-node-public-alpha)
    version=$(jq -er 'select(.decision_id=="DEC-002-single-node-public-alpha-20261002" and .profile=="single-node-public-alpha") | .release_version | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+-alpha\\.[0-9]+$"))' docs/plans/next-release/qualification-policy.json)
    tag_commit=$(git rev-parse --verify "refs/tags/$version^{commit}" 2>/dev/null) || { echo "build-candidate: release tag must identify candidate HEAD" >&2; exit 2; }
    [[ $tag_commit == "$(git rev-parse HEAD)" ]] || { echo "build-candidate: release tag must identify candidate HEAD" >&2; exit 2; }
    mkdir -p "$(dirname "$out")"
    GOFLAGS= GOWORK=off CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags "-s -w -X main.version=$version" -o "$out" ./cmd/instantd
    ;;
  *) echo "build-candidate: unknown profile" >&2;exit 2;;
esac
