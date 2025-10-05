-- 1) Connection targets / engines
CREATE TABLE control.systems (
  id                BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL UNIQUE,
  kind              TEXT NOT NULL,
  catalog           TEXT,
  host              TEXT,
  port              INTEGER,
  database          TEXT,
  user_secret_key   TEXT,
  pass_secret_key   TEXT,
  jdbc_string       TEXT,
  options           JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  created_by        TEXT NOT NULL,
  updated_by        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  version           INTEGER NOT NULL DEFAULT 1
);

-- 2) Named table ↔ table comparisons (schema-driven)
CREATE TABLE control.datasets (
  id               BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,         -- "sales_daily_tbl"
  src_system_id    BIGINT NOT NULL REFERENCES control.systems(id),
  src_schema       TEXT,
  src_table        TEXT,
  tgt_system_id    BIGINT NOT NULL REFERENCES control.systems(id),
  tgt_schema       TEXT,
  tgt_table        TEXT,

  compare_mode     TEXT NOT NULL DEFAULT 'except_all',   -- 'except_all' | 'primary_key' | 'hash'
  pk_columns       TEXT[] DEFAULT NULL,                  -- for primary_key mode
  watermark_column TEXT DEFAULT NULL,                    -- optional time/seq filter
  include_columns  TEXT[] NOT NULL DEFAULT '{}',         -- default: compare all
  exclude_columns  TEXT[] NOT NULL DEFAULT '{}',
  options          JSONB NOT NULL DEFAULT '{}'::jsonb,   -- tolerances, null eq, coercions
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,

  created_by       TEXT NOT NULL,
  updated_by       TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  version          INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX ON control.datasets (src_system_id);
CREATE INDEX ON control.datasets (tgt_system_id);
CREATE INDEX datasets_active_idx ON control.datasets (name) WHERE is_active;

-- 3) Arbitrary SQL ↔ SQL comparisons
CREATE TABLE control.compare_queries (
  id               BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,         -- "daily_sales_sql"
  sql              TEXT NOT NULL,
  src_system_id    BIGINT NOT NULL REFERENCES control.systems(id),
  tgt_system_id    BIGINT NOT NULL REFERENCES control.systems(id),

  compare_mode     TEXT NOT NULL DEFAULT 'except_all',   -- same options as datasets
  pk_columns       TEXT[] DEFAULT NULL,
  include_columns  TEXT[] NOT NULL DEFAULT '{}',
  exclude_columns  TEXT[] NOT NULL DEFAULT '{}',
  options          JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,

  created_by       TEXT NOT NULL,
  updated_by       TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  version          INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX compare_queries_active_idx ON control.compare_queries (name) WHERE is_active;
CREATE INDEX compare_queries_src_tgt_idx ON control.compare_queries (src_system_id, tgt_system_id);

-- 4) Schedules (cron-ish) + state
CREATE TABLE control.schedules (
  id               BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,
  cron_expr        TEXT NOT NULL,                -- e.g., "0 2 * * TUE,THU"
  timezone         TEXT NOT NULL DEFAULT 'UTC',
  enabled          BOOLEAN NOT NULL DEFAULT TRUE,
  max_concurrency  INTEGER NOT NULL DEFAULT 4,   -- per-schedule limit
  backfill_policy  TEXT NOT NULL DEFAULT 'none', -- 'none' | 'catch_up' | 'skip_missed'

  -- scheduler-maintained cursors (optional but pragmatic)
  last_run_at      TIMESTAMPTZ,
  next_run_at      TIMESTAMPTZ,

  created_by       TEXT NOT NULL,
  updated_by       TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  version          INTEGER NOT NULL DEFAULT 1
);

-- 5) Attach schedules to datasets or compare_queries
CREATE TABLE control.schedule_bindings (
  id               BIGSERIAL PRIMARY KEY,
  schedule_id      BIGINT NOT NULL REFERENCES control.schedules(id) ON DELETE CASCADE,
  entity_type      TEXT  NOT NULL,               -- 'dataset' | 'compare_query'
  entity_id        BIGINT NOT NULL,              -- FK by app logic
  UNIQUE (schedule_id, entity_type, entity_id)
);

-- 6) Triggers / run queue (manual & scheduled). Workers pop SKIP LOCKED.
CREATE TABLE control.triggers (
  id               BIGSERIAL PRIMARY KEY,
  source           TEXT NOT NULL,                -- 'manual' | 'schedule' | 'bulk_job'
  schedule_id      BIGINT REFERENCES control.schedules(id),
  entity_type      TEXT NOT NULL,                -- 'dataset' | 'compare_query'
  entity_id        BIGINT NOT NULL,

  priority         INTEGER NOT NULL DEFAULT 100, -- lower = sooner
  requested_by     TEXT NOT NULL,
  requested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  status           TEXT NOT NULL DEFAULT 'queued',  -- 'queued'|'running'|'succeeded'|'failed'|'canceled'
  attempts         INTEGER NOT NULL DEFAULT 0,
  last_error       TEXT,
  worker_id        TEXT,                         -- advisory: who picked it
  locked_at        TIMESTAMPTZ,                  -- FOR UPDATE SKIP LOCKED timestamp
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,

  params           JSONB NOT NULL DEFAULT '{}'::jsonb -- watermark overrides, dry-run, etc.
);

CREATE INDEX triggers_ready_idx ON control.triggers (status, priority);

-- 8) Tags for organization / filters in UI
CREATE TABLE control.tags (
  id    BIGSERIAL PRIMARY KEY,
  name  TEXT NOT NULL UNIQUE
);

CREATE TABLE control.entity_tags (
  entity_type TEXT NOT NULL,
  entity_id   BIGINT NOT NULL,
  tag_id      BIGINT NOT NULL REFERENCES control.tags(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id, tag_id)
);
