-- +goose Up
-- Registered OAuth redirect origins per app (v1 apps.redirect_origins).
-- Stored as a JSONB array of "scheme://host" strings. OAuth start/callback
-- refuse any redirect_uri whose origin is not listed here — an empty list
-- means the app has no permitted redirects (fail-closed).

ALTER TABLE apps
  ADD COLUMN IF NOT EXISTS redirect_origins jsonb NOT NULL DEFAULT '[]'::jsonb;

-- +goose Down
ALTER TABLE apps
  DROP COLUMN IF EXISTS redirect_origins;
