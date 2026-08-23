-- +goose Up
-- Consolidated port of v1 migrations (see docs/01-state.md):
--   01_bootstrap + 05_app_admin_tokens + 06_fix_ref_constraint
--   + 36_checked_data_type (enum + extract/validity functions)
--   + 53_change_indexing_of_triple_nulls (av_index via json_null_to_null)
--   + 64_better_uuid_constraint (triples_extract_uuid_value)
--   + 68_attrs_etype_label_schema (etype/label onto attrs + name triggers)
--   + 73_vae_uuids (json_uuid_to_uuid)
-- Deferred (tracked in docs/04-roadmap.md): oauth login tables, profiles,
-- history/email-verification/webhook tables, rule_versions history trigger,
-- indexing_jobs, wal_logs audit partitions (invalidation flows through
-- logical replication in v2; see docs/02-architecture.md §5).

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

-- checked_data_type enum (v1 migration 36)
create type checked_data_type as enum ('string', 'number', 'boolean', 'date');

CREATE TABLE attrs(
  id uuid PRIMARY KEY,
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,

  etype text,
  label text,
  reverse_etype text,
  reverse_label text,

  value_type text NOT NULL, -- 'ref' | 'blob'
  cardinality text NOT NULL, -- 'many' | 'one'
  is_unique boolean NOT NULL,
  is_indexed boolean NOT NULL,

  forward_ident uuid NOT NULL,
  reverse_ident uuid,

  checked_data_type checked_data_type,
  checking_data_type boolean,
  indexing boolean,

  deletion_marked_at timestamp with time zone
);

ALTER TABLE attrs
ADD CONSTRAINT attrs_etype_label_unique UNIQUE (app_id, etype, label);

ALTER TABLE attrs
ADD CONSTRAINT attrs_reverse_etype_label_unique UNIQUE (app_id, reverse_etype, reverse_label);

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

  checked_data_type checked_data_type,

  PRIMARY KEY(app_id, entity_id, attr_id, value_md5)
);

CREATE INDEX triples_app_id ON triples (app_id);

CREATE INDEX triples_attr_id ON triples (attr_id);

-- ---- Custom SQL functions (ports of v1 migrations 36/53/64/73) ----

-- +goose StatementBegin
create or replace function public.json_uuid_to_uuid(v jsonb) returns uuid
  language sql
  parallel safe
  immutable
  as $$
   select (v->>0)::uuid
  $$;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.json_null_to_null(v jsonb) returns jsonb
  language sql immutable
  as $$
    select case
      when v = 'null'::jsonb then null
      else v
    end
  $$;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_extract_uuid_value(value jsonb)
 returns uuid
 language sql
 immutable
as $$
  select case
    when jsonb_typeof(value) = 'string'
     and pg_input_is_valid(value #>> '{}', 'uuid')
    then (value #>> '{}')::uuid
    else null
  end
$$;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_extract_string_value(value jsonb)
returns text as $$
  select case
    when jsonb_typeof(value) = 'string' then (value->>0)::text
    else null
  end
$$ language sql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_extract_number_value(value jsonb)
returns double precision as $$
  select case
    when jsonb_typeof(value) = 'number' then (value->>0)::double precision
    else null
  end
$$ language sql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_extract_boolean_value(value jsonb)
returns boolean as $$
  select case
    when jsonb_typeof(value) = 'boolean' then (value->>0)::boolean
    else null
  end
$$ language sql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_extract_date_value(value jsonb)
returns timestamp with time zone as $$
  select case jsonb_typeof(value)
    when 'number' then to_timestamp((value->>0)::double precision / 1000)
    when 'string' then ((value->>0)::text)::timestamp with time zone
    else null
  end
$$ language sql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.is_jsonb_valid_timestamp(value jsonb)
returns boolean as $$
  declare
    ts timestamp with time zone;
  begin
    begin
      if (jsonb_typeof(value) = 'number') then
        ts := triples_extract_date_value(value);
        return true;
      else
        return false;
      end if;
    exception when others then
      return false;
    end;
  end
$$ language plpgsql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.is_jsonb_valid_datestring(value jsonb)
returns boolean as $$
  declare
    ts timestamp with time zone;
  begin
    begin
      if jsonb_typeof(value) = 'string' then
        ts := triples_extract_date_value(value);
        return true;
      else
        return false;
      end if;
    exception when others then
      return false;
    end;
  end
$$ language plpgsql immutable;
-- +goose StatementEnd

-- +goose StatementBegin
create or replace function public.triples_valid_value(data_type checked_data_type, value jsonb)
returns boolean as $$
  begin
    case data_type
      when 'string' then
        return jsonb_typeof(value) in ('string', 'null');
      when 'number' then
        return jsonb_typeof(value) in ('number', 'null');
      when 'boolean' then
        return jsonb_typeof(value) in ('boolean', 'null');
      when 'date' then
        case jsonb_typeof(value)
          when 'null' then return true;
          when 'number' then return is_jsonb_valid_timestamp(value);
          when 'string' then return is_jsonb_valid_datestring(value);
          else return false;
        end case;
      else
        return data_type is null;
    end case;
  end
$$ language plpgsql immutable;
-- +goose StatementEnd


-- Index-flag columns drive partial indexes; queries must name one of these
-- five access paths (parity with db/datalog.clj pattern index requirements).
CREATE UNIQUE INDEX ea_index
  ON triples(app_id, entity_id, attr_id)
  WHERE ea;

-- eav_index: refs are stored as JSON strings of uuid text (v1 migration 06
-- semantics: jsonb_typeof='string' AND (value->>0)::uuid IS NOT NULL).
CREATE UNIQUE INDEX eav_index
  ON triples(app_id, entity_id, attr_id, value)
  WHERE eav;

-- av_index ignores jsonb nulls so multiple entities may hold null for a
-- unique attr (v1 migration 53).
CREATE UNIQUE INDEX av_ignore_nulls_index
  ON triples(app_id, attr_id, json_null_to_null(value))
  INCLUDE (entity_id)
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

ALTER TABLE triples
  ADD CONSTRAINT valid_value_data_type
    CHECK (triples_valid_value(checked_data_type, value));

-- Attr forward/reverse name-collision guard (v1 migration 68)
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION check_attrs_unique_names ()
  RETURNS TRIGGER
  AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM attrs
     WHERE (app_id, reverse_etype, reverse_label) = (NEW.app_id, NEW.etype, NEW.label)
       AND id <> NEW.id
  ) THEN
    RAISE EXCEPTION 'trigger violation trg_attrs_unique_names';
  END IF;
  IF EXISTS (
    SELECT 1 FROM attrs
     WHERE (app_id, etype, label) = (NEW.app_id, NEW.reverse_etype, NEW.reverse_label)
       AND id <> NEW.id
  ) THEN
    RAISE EXCEPTION 'trigger violation trg_attrs_unique_names';
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER trg_attrs_unique_names
BEFORE INSERT OR UPDATE ON attrs
FOR EACH ROW EXECUTE PROCEDURE check_attrs_unique_names();
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS triples;
DROP TABLE IF EXISTS idents;
DROP TABLE IF EXISTS attrs;
DROP FUNCTION IF EXISTS check_attrs_unique_names();
DROP TRIGGER IF EXISTS trg_attrs_unique_names ON attrs;
DROP FUNCTION IF EXISTS triples_valid_value(checked_data_type, jsonb);
DROP FUNCTION IF EXISTS is_jsonb_valid_datestring(jsonb);
DROP FUNCTION IF EXISTS is_jsonb_valid_timestamp(jsonb);
DROP FUNCTION IF EXISTS triples_extract_date_value(jsonb);
DROP FUNCTION IF EXISTS triples_extract_boolean_value(jsonb);
DROP FUNCTION IF EXISTS triples_extract_number_value(jsonb);
DROP FUNCTION IF EXISTS triples_extract_string_value(jsonb);
DROP FUNCTION IF EXISTS triples_extract_uuid_value(jsonb);
DROP FUNCTION IF EXISTS json_null_to_null(jsonb);
DROP FUNCTION IF EXISTS json_uuid_to_uuid(jsonb);
DROP TYPE IF EXISTS checked_data_type;
DROP TABLE IF EXISTS app_user_magic_codes;
DROP TABLE IF EXISTS app_user_refresh_tokens;
DROP TABLE IF EXISTS app_users;
DROP TABLE IF EXISTS app_admin_tokens;
DROP TABLE IF EXISTS apps;
DROP TABLE IF EXISTS instant_user_outreaches;
DROP TABLE IF EXISTS instant_user_magic_codes;
DROP TABLE IF EXISTS instant_user_refresh_tokens;
DROP TABLE IF EXISTS instant_users;
