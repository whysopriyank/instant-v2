-- +goose Up
-- Consolidated port of v1 migrations 01_bootstrap + 05_add_app_admin_tokens,
-- with the 06_fix_ref_constraint_on_triples correction applied inline.
-- Deferred to later phases (tracked in docs/04-roadmap.md): oauth login tables,
-- profiles, history/email-verification/webhook tables, rule_versions history.

CREATE TABLE instant_users(
  id uuid PRIMARY KEY,
  email text NOT NULL UNIQUE,
  created_at timestamp DEFAULT NOW()
);

CREATE TABLE instant_user_refresh_tokens(
  id uuid primary key,
  user_id uuid NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_instant_user_id
    FOREIGN KEY(user_id)
    REFERENCES instant_users(id)
    ON DELETE CASCADE
);

CREATE TABLE instant_user_magic_codes(
  id uuid primary key,
  code text NOT null,
  user_id uuid NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_user_id
    FOREIGN KEY(user_id)
    REFERENCES instant_users(id)
    ON DELETE CASCADE
);

CREATE TABLE instant_user_outreaches (
  user_id uuid PRIMARY KEY,
  created_at TIMESTAMP DEFAULT NOW(),
  CONSTRAINT fk_user_id
    FOREIGN KEY(user_id)
    REFERENCES instant_users(id)
    ON DELETE CASCADE
);

CREATE TABLE apps(
  id uuid PRIMARY KEY,
  creator_id uuid NOT NULL references instant_users(id) ON DELETE CASCADE,
  title text NOT NULL,
  created_at timestamp DEFAULT NOW()
);

CREATE INDEX apps_creator_id ON apps (creator_id);

CREATE TABLE app_admin_tokens(
  token uuid primary key,
  app_id uuid NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_app_id
    FOREIGN KEY(app_id)
    REFERENCES apps(id)
    ON DELETE CASCADE
);

CREATE TABLE app_users(
  id uuid PRIMARY KEY,
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
  email text NOT NULL,
  CONSTRAINT app_id_email_uq UNIQUE (app_id, email),
  created_at timestamp DEFAULT NOW()
);

CREATE TABLE app_user_refresh_tokens(
  id uuid primary key,
  user_id uuid NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_app_user_id
    FOREIGN KEY(user_id)
    REFERENCES app_users(id)
    ON DELETE CASCADE
);

CREATE TABLE app_user_magic_codes(
  id uuid primary key,
  code text NOT null,
  user_id uuid NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  FOREIGN KEY(user_id) REFERENCES app_users(id) ON DELETE CASCADE
);

CREATE TABLE attrs(
  id uuid PRIMARY KEY,
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,

  value_type text NOT NULL, -- 'ref' | 'blob'
  cardinality text NOT NULL, -- 'many' | 'one'
  is_unique boolean NOT NULL,
  is_indexed boolean NOT NULL,

  forward_ident uuid NOT NULL,
  reverse_ident uuid
);

CREATE INDEX attrs_app_id ON attrs (app_id);
CREATE INDEX attrs_forward_ident ON attrs (forward_ident);
CREATE INDEX attrs_reverse_ident ON attrs (reverse_ident);

CREATE TABLE idents (
  id uuid PRIMARY KEY,
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
  attr_id uuid NOT NULL REFERENCES attrs(id) ON DELETE CASCADE,
  etype text NOT NULL,
  label text NOT NULL,

  CONSTRAINT app_ident_uq UNIQUE (app_id, etype, label)
);

CREATE INDEX idents_app_id ON idents (app_id);
CREATE INDEX idents_attr_id ON idents (attr_id);

CREATE TABLE triples(
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,

  entity_id uuid NOT NULL,

  attr_id uuid REFERENCES attrs(id) ON DELETE CASCADE,

  value jsonb NOT NULL,

  value_md5 text NOT NULL,

  ea boolean NOT NULL,
  eav boolean NOT NULL,

  av boolean NOT NULL,
  ave boolean NOT NULL,

  vae boolean NOT NULL,

  PRIMARY KEY(app_id, entity_id, attr_id, value_md5)
);

CREATE INDEX triples_app_id ON triples (app_id);

CREATE INDEX triples_attr_id ON triples (attr_id);

-- Index-flag columns drive partial indexes; queries must name one of these
-- five access paths (parity with db/datalog.clj pattern index requirements).
CREATE UNIQUE INDEX ea_index
  ON triples(app_id, entity_id, attr_id)
  WHERE ea;

CREATE UNIQUE INDEX eav_index
  ON triples(app_id, entity_id, attr_id, value)
  WHERE eav;

CREATE UNIQUE INDEX av_index
  ON triples(app_id, attr_id, value) INCLUDE (entity_id)
  WHERE av;

CREATE INDEX ave_index
  ON triples(app_id, attr_id, value, entity_id)
  WHERE ave;

CREATE INDEX vae_index
  ON triples(app_id, value, attr_id, entity_id)
  WHERE vae;

ALTER TABLE triples
  ADD CONSTRAINT ref_values_are_uuid
  CHECK (
    CASE WHEN eav OR vae THEN
        jsonb_typeof(value) = 'string' AND
        (value->>0)::uuid IS NOT NULL
    ELSE TRUE
    END
  );

ALTER TABLE triples
  ADD CONSTRAINT indexed_values_are_constrained
  CHECK (
    CASE WHEN eav OR av OR ave OR vae THEN
        pg_column_size(value) <= 1024
    ELSE TRUE
    END
  );

-- +goose Down
DROP TABLE IF EXISTS triples;
DROP TABLE IF EXISTS idents;
DROP TABLE IF EXISTS attrs;
DROP TABLE IF EXISTS app_user_magic_codes;
DROP TABLE IF EXISTS app_user_refresh_tokens;
DROP TABLE IF EXISTS app_users;
DROP TABLE IF EXISTS app_admin_tokens;
DROP TABLE IF EXISTS apps;
DROP TABLE IF EXISTS instant_user_outreaches;
DROP TABLE IF EXISTS instant_user_magic_codes;
DROP TABLE IF EXISTS instant_user_refresh_tokens;
DROP TABLE IF EXISTS instant_users;
