-- +goose Up
-- Audit F4/M4: brute-force lockout and resend-throttle state must be shared
-- across nodes so N replicas enforce ONE budget per (app,email), not N.
-- Replaces the process-local attempts map (which also grew without bound).
CREATE TABLE IF NOT EXISTS auth_throttle(
  app_id uuid NOT NULL,
  key text NOT NULL,                 -- lower(trimmed email)
  fails int NOT NULL DEFAULT 0,
  locked_until timestamptz NOT NULL DEFAULT to_timestamp(0),
  last_sent timestamptz NOT NULL DEFAULT to_timestamp(0),
  PRIMARY KEY (app_id, key)
);

-- +goose Down
DROP TABLE IF EXISTS auth_throttle;
