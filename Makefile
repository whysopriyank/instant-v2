SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GO      ?= go
LINT    ?= golangci-lint
MIGRATE ?= goose

V1_REF  ?= a4d2ef33          # instantdb/instant sunset commit
V1_PATH ?= ../instant

.PHONY: help bootstrap lint test vet tidy corpus corpus-check replay differential build run migrate-up migrate-down clean

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
	$(GO) build -o bin/instantd ./cmd/instantd

run: build ## run instantd (reads env)
	./bin/instantd

schemagen: ## regenerate wire types from the protocol schema
	$(GO) run ./tools/schemagen --schema internal/protocol/schema/protocol.schema.json --out internal/protocol/

clean:
	rm -rf bin dist
