-- +goose Up
-- Port of v1 migrations 04_add_rules + 102_rule_versions (version column only;
-- editscript history trigger deferred until Phase 2 needs it).

CREATE TABLE rules (
  app_id uuid PRIMARY KEY REFERENCES apps(id) ON DELETE CASCADE,
  code jsonb NOT NULL,
  version integer not null default 0
);

CREATE INDEX rules_app_id ON rules (app_id);

-- +goose Down
DROP TABLE IF EXISTS rules;
