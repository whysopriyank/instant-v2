-- +goose Up
-- Port of v1 migration 03_add_transactions: per-app transaction journal that
-- feeds the WAL tailer ordering guarantees (docs/02-architecture.md §5).

CREATE TABLE transactions (
  id BIGINT primary key GENERATED ALWAYS AS IDENTITY,
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS transactions;
