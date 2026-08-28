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

BENCH_PACKAGES := ./internal/benchharness ./internal/benchrun ./tools/soak ./tools/soaksetup ./tools/benchrun ./tools/benchreport

.PHONY: help bootstrap lint test vet tidy corpus corpus-check replay differential build build-bench bench-acceptance bench-run bench-report bench-verify bench-smoke run migrate-up migrate-down clean schemagen

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "%-18s %s\n",$$1,$$2}'

bootstrap: ## create toolchain state (no logic yet)
	$(GO) mod tidy
	@mkdir -p bin corpus internal/protocol/schema migrations

lint: ## static checks for the current phase's packages
	$(LINT) run ./... || true   # advisory until Phase 1

vet: ## go vet
	$(GO) vet ./...

test: ## unit tests (scoped per package; corpus suites are separate)
	$(GO) test ./... -race -count=1 -short

tidy: ## keep go.mod honest
	$(GO) mod tidy

corpus: ## (re)record corpus/ against the live v1 at V1_PATH
	$(GO) run ./tools/corpusctl --mode record --v1-path $(V1_PATH) --v1-ref $(V1_REF) --out corpus/

corpus-check: ## replay corpus/ against v1 — must be green before any ported logic
	$(GO) run ./tools/corpusctl --mode replay --target v1 --v1-path $(V1_PATH) --corpus corpus/

replay: ## replay against a target: make replay TARGET=v2 SUITE=03-query
	$(GO) run ./tools/corpusctl --mode replay --target $(or $(TARGET),v2) --suite $(or $(SUITE),) --corpus corpus/

differential: ## side-by-side v1 vs v2 on a suite: make differential SUITE=04-transact
	$(GO) run ./tools/corpusctl --mode differential --v1-path $(V1_PATH) --suite $(SUITE) --corpus corpus/

migrate-up: ## apply migrations into $$DATABASE_URL (or testcontainers in CI)
	$(MIGRATE) -dir migrations postgres "$$DATABASE_URL" up

migrate-down:
	$(MIGRATE) -dir migrations postgres "$$DATABASE_URL" down

build: ## build instantd
	$(GO) build -o $(BIN)/instantd ./cmd/instantd

build-bench: ## build instantd and benchmark tools
	@mkdir -p "$(BIN)"
	$(GO) build -o "$(BIN)/instantd" ./cmd/instantd
	$(GO) build -o "$(BIN)/benchrun" ./tools/benchrun
	$(GO) build -o "$(BIN)/benchreport" ./tools/benchreport
	$(GO) build -o "$(BIN)/soak" ./tools/soak
	$(GO) build -o "$(BIN)/soaksetup" ./tools/soaksetup

bench-acceptance: ## run benchmark harness/report acceptance tests with race detection
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

bench-smoke: build-bench ## prepare a marked disposable DB for an operator-invoked V2 soak
	test -n "$(DATABASE_URL)" || { echo "DATABASE_URL is required" >&2; exit 2; }
	test -n "$(BENCHMARK_MARKER)" || { echo "BENCHMARK_MARKER is required" >&2; exit 2; }
	@test "$(DATABASE_URL)" = "$$(printf '%s' "$(DATABASE_URL)" | grep -E 'instant_bench_[A-Za-z0-9_.-]+')" || { echo "DATABASE_URL must name an instant_bench_ database" >&2; exit 2; }
	"$(BIN)/soaksetup" -database-url "$(DATABASE_URL)" -marker "$(BENCHMARK_MARKER)"

run: build ## run instantd (reads env)
	"$(BIN)/instantd"

schemagen: ## regenerate wire types from the protocol schema
	$(GO) run ./tools/schemagen --schema internal/protocol/schema/protocol.schema.json --out internal/protocol/

clean:
	rm -rf bin dist
