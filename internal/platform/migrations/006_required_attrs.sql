-- +goose Up
-- Port of v1 migration 59_is_required. NULL/default-free reads are avoided in
-- the Go catalog by keeping the flag non-null for every attr, including rows
-- created before required-attribute support shipped.
ALTER TABLE attrs
  ADD COLUMN IF NOT EXISTS is_required boolean NOT NULL DEFAULT false;

UPDATE attrs
   SET is_required = true
 WHERE label = 'id';

-- +goose Down
ALTER TABLE attrs
  DROP COLUMN IF EXISTS is_required;
