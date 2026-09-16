SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GO      ?= go
LINT    ?= golangci-lint
MIGRATE ?= goose

V1_REF  ?= a4d2ef33          # instantdb/instant sunset commit
V1_PATH ?= ../instant
BIN     ?= bin
BUNDLE  ?= benchmarks/results/manual
SEED    ?= 1
PAIR    ?= pair-1
FAMILY  ?= H-append
SCALE   ?= 300
BENCH_CONFIG ?=
SYNTHETIC ?= 0
INTEGRATION_PARALLEL ?= 4
SOAK_URL ?= ws://127.0.0.1:8888/runtime/session
SOAK_EVENTS ?= soak-events.jsonl
SOAK_LOG ?= soak.log
V1_URL ?=
V2_URL ?=
DIFFERENTIAL_OUTPUT ?=

# Supported benchmark packages tested by bench-acceptance.
# Per DEC-001 (EV-006), cmd/benchsmoke is declared historical diagnostic code
# and is not a supported acceptance entrypoint; it is deliberately excluded here.
BENCH_PACKAGES := ./internal/benchharness ./internal/benchrun ./cmd/soak ./cmd/soaksetup ./cmd/benchrun ./cmd/benchreport

.PHONY: help bootstrap lint test test-unit test-release-contract supply-chain-preflight test-supply-chain-contract test-integration test-contract test-contract-discovery test-release validate-release require-integration container-verify soak-gate vet tidy corpus corpus-check replay differential build build-bench build-historical-benchsmoke bench-acceptance bench-run bench-report bench-verify bench-smoke run migrate-up migrate-down clean schemagen

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "%-18s %s\n",$$1,$$2}'

bootstrap: ## create toolchain state (no logic yet)
	$(GO) mod tidy
	@mkdir -p bin corpus

lint: ## required static checks (linter failures fail the target)
	$(LINT) run ./...

vet: ## go vet
	$(GO) vet ./...

test: test-unit ## alias for the hermetic unit lane

test-unit: test-release-contract ## unit/race tests without live PostgreSQL prerequisites
	INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= $(GO) test ./... -race -count=1 -short

test-release-contract: ## hermetic QR-003 release-gate contract checks
	bash scripts/test-quality-release-gate.sh
	GOFLAGS=-json $(MAKE) --no-print-directory test-contract-discovery

supply-chain-preflight: ## offline QR-005 supply-chain input preflight
	bash scripts/quality-supply-chain-preflight.sh

test-supply-chain-contract: ## hermetic QR-005 supply-chain preflight contract checks
	bash scripts/test-quality-supply-chain-preflight.sh

require-integration:
	@test -n "$${DATABASE_URL:-}" || { echo "DATABASE_URL is required; use an isolated PostgreSQL server with CREATEDB and wal_level=logical" >&2; exit 2; }

test-integration: require-integration ## explicitly required isolated PostgreSQL suites
	bash scripts/quality-integration.sh
	INSTANT_TEST_INTEGRATION=1 $(GO) test ./... -race -count=1 -p $(INTEGRATION_PARALLEL)

test-contract: require-integration check-generated ## validate corpus and replay real v2 behavior with isolated fixtures
	$(GO) run ./cmd/corpusctl --mode validate --corpus corpus/
	INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= $(GO) test ./internal/corpus ./cmd/corpusctl ./internal/protocol -race -count=1 -short
	$(MAKE) --no-print-directory test-contract-discovery
	INSTANT_TEST_INTEGRATION=1 $(GO) test ./internal/corpus -run '^TestCorpusReplayIntegration$$' -race -count=1

test-contract-discovery: ## prove the selected integration test exists, even when the parent gate uses JSON output
	GOFLAGS= $(GO) test ./internal/corpus -list '^TestCorpusReplayIntegration$$' | grep -Fx 'TestCorpusReplayIntegration'

container-verify: ## build and check container health, non-root identity, and TLS roots
	bash scripts/quality-container.sh

soak-gate: build-bench ## existing CI soak/RSS gate against an explicitly seeded running server
	SOAK_BIN="$(BIN)/soak" SOAK_URL="$(SOAK_URL)" SOAK_APP="$(SOAK_APP)" SOAK_ATTR="$(SOAK_ATTR)" SOAK_SERVER_PID="$(SOAK_SERVER_PID)" SOAK_EVENTS="$(SOAK_EVENTS)" SOAK_LOG="$(SOAK_LOG)" bash scripts/quality-soak.sh

test-release: ## DEC-001 single-node-alpha gate; validates pre-existing qualified evidence
	bash scripts/quality-release-gate.sh

validate-release: ## validate the approved dynamic-view exclusion against corpus and release envelope
	$(GO) run ./cmd/corpusctl --mode validate-release --corpus corpus/ --release-envelope docs/reference/release-envelope.md

tidy: ## keep go.mod honest
	$(GO) mod tidy

corpus: ## report the explicit unavailable v1 recording capability
	$(GO) run ./cmd/corpusctl --mode record --v1-path "$(V1_PATH)" --v1-ref "$(strip $(V1_REF))" --corpus corpus/

corpus-check: ## replay against an explicitly supplied, seeded v1 WebSocket endpoint
	@test -n "$(V1_URL)" || { echo "V1_URL must be a seeded WebSocket endpoint" >&2; exit 2; }
	$(GO) run ./cmd/corpusctl --mode replay --target "$(V1_URL)" --suite "$(SUITE)" --corpus corpus/

replay: ## replay against an explicit WebSocket TARGET; optional SUITE filter
	@test -n "$(TARGET)" || { echo "TARGET must be a seeded WebSocket endpoint" >&2; exit 2; }
	$(GO) run ./cmd/corpusctl --mode replay --target "$(TARGET)" --suite "$(SUITE)" --corpus corpus/

differential: ## live pinned-v1/v2 comparison; both endpoints must have equivalent seeded fixtures
	@test -n "$(V1_URL)" && test -n "$(V2_URL)" || { echo "V1_URL and V2_URL WebSocket endpoints with equivalent seeded fixtures are required" >&2; exit 2; }
	@test -n "$(DIFFERENTIAL_OUTPUT)" || { echo "DIFFERENTIAL_OUTPUT must name a fresh private evidence directory" >&2; exit 2; }
	set -e; \
	actual=$$(git -C "$(V1_PATH)" rev-parse HEAD); \
	expected=$$(git -C "$(V1_PATH)" rev-parse "$(strip $(V1_REF))^{commit}"); \
	test "$$actual" = "$$expected" || { echo "V1_PATH checkout must match V1_REF" >&2; exit 2; }; \
	$(GO) run ./cmd/corpusctl --mode differential --target "$(V2_URL)" --other "$(V1_URL)" --v1-path "$(V1_PATH)" --v1-ref "$$expected" --output-dir "$(DIFFERENTIAL_OUTPUT)" --suite "$(SUITE)" --corpus corpus/

migrate-up: ## apply migrations into $$DATABASE_URL (or testcontainers in CI)
	$(MIGRATE) -dir internal/platform/migrations postgres "$$DATABASE_URL" up

migrate-down:
	$(MIGRATE) -dir internal/platform/migrations postgres "$$DATABASE_URL" down

build: ## build instantd
	$(GO) build -o $(BIN)/instantd ./cmd/instantd

build-bench: ## build instantd and supported benchmark tools (excludes historical benchsmoke)
	@mkdir -p "$(BIN)"
	$(GO) build -o "$(BIN)/instantd" ./cmd/instantd
	$(GO) build -o "$(BIN)/benchrun" ./cmd/benchrun
	$(GO) build -o "$(BIN)/benchreport" ./cmd/benchreport
	$(GO) build -o "$(BIN)/soak" ./cmd/soak
	$(GO) build -o "$(BIN)/soaksetup" ./cmd/soaksetup

build-historical-benchsmoke: ## compile historical diagnostic benchsmoke CLI (unsupported acceptance entrypoint; see DEC-001)
	@mkdir -p "$(BIN)"
	$(GO) build -o "$(BIN)/benchsmoke" ./cmd/benchsmoke

bench-acceptance: ## run supported benchmark harness/report acceptance tests with race detection (excludes historical benchsmoke)
	$(GO) test $(BENCH_PACKAGES) -race -count=1 -short -p 1

bench-run: build-bench ## run an explicit live-config pair or synthetic acceptance pair
	@test -n "$(BUNDLE)" || { echo "BUNDLE is required" >&2; exit 2; }
	@if test -n "$(BENCH_CONFIG)" && test "$(SYNTHETIC)" = 1; then echo "BENCH_CONFIG and SYNTHETIC=1 are mutually exclusive" >&2; exit 2; fi
	@if test -n "$(BENCH_CONFIG)"; then \
		"$(BIN)/benchrun" -config "$(BENCH_CONFIG)" -output "$(BUNDLE)/raw"; \
	elif test "$(SYNTHETIC)" = 1; then \
		"$(BIN)/benchrun" -synthetic -output "$(BUNDLE)/raw" -seed "$(SEED)" -pair "$(PAIR)" -family "$(FAMILY)" -scale "$(SCALE)"; \
	else \
		echo "set BENCH_CONFIG=/path/live-config.json or SYNTHETIC=1; refusing an implicit benchmark mode" >&2; exit 2; \
	fi

bench-report: build-bench ## verify a raw bundle and render its offline report
	test -f "$(BUNDLE)/raw/raw-index.json"
	"$(BIN)/benchreport" -input "$(BUNDLE)/raw" -output "$(BUNDLE)/report.md"

bench-verify: bench-report ## verify a bundle and regenerate its offline report
	test -s "$(BUNDLE)/report.md"

bench-smoke: build-bench ## prepare a marked disposable DB via supported cmd/soaksetup (cmd/benchsmoke is historical; DEC-001)
	test -n "$(DATABASE_URL)" || { echo "DATABASE_URL is required" >&2; exit 2; }
	test -n "$(BENCHMARK_MARKER)" || { echo "BENCHMARK_MARKER is required" >&2; exit 2; }
	@test "$(DATABASE_URL)" = "$$(printf '%s' "$(DATABASE_URL)" | grep -E 'instant_bench_[A-Za-z0-9_.-]+')" || { echo "DATABASE_URL must name an instant_bench_ database" >&2; exit 2; }
	"$(BIN)/soaksetup" -database-url "$(DATABASE_URL)" -marker "$(BENCHMARK_MARKER)"

run: build ## run instantd (reads env)
	"$(BIN)/instantd"

schemagen: ## regenerate wire types from the protocol schema
	$(GO) run ./cmd/schemagen --schema internal/protocol/schema/protocol.schema.json --out internal/protocol/

.PHONY: check-generated
check-generated: ## verify generated protocol artifacts are reproducible
	GO="$(GO)" bash scripts/quality-generated.sh

clean:
	rm -rf bin dist
