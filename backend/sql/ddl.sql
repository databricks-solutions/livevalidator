-- 1) Connection targets / engines
CREATE TABLE control.systems (
  id                BIGSERIAL,
  name              TEXT NOT NULL UNIQUE,
  kind              TEXT NOT NULL,
  catalog           TEXT,
  host              TEXT,
  port              INTEGER,
  database          TEXT,
  user_secret_key   TEXT,
  pass_secret_key   TEXT,
  jdbc_string       TEXT,
  concurrency       INTEGER NOT NULL DEFAULT -1,
  options           JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  created_by        TEXT NOT NULL,
  updated_by        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  version           INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (id, name)
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
  config_overrides JSONB DEFAULT NULL,                   -- entity-specific validation config overrides

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
  options          JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  config_overrides JSONB DEFAULT NULL,                   -- entity-specific validation config overrides

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
  entity_type      TEXT  NOT NULL,               -- 'table' | 'compare_query'
  entity_id        BIGINT NOT NULL,              -- FK by app logic
  UNIQUE (schedule_id, entity_type, entity_id)
);

-- 6) Triggers / run queue (manual & scheduled). Workers pop SKIP LOCKED.
CREATE TABLE control.triggers (
  id               BIGSERIAL PRIMARY KEY,
  source           TEXT NOT NULL,                -- 'manual' | 'schedule' | 'bulk_job'
  schedule_id      BIGINT REFERENCES control.schedules(id),
  entity_type      TEXT NOT NULL,                -- 'table' | 'compare_query'
  entity_id        BIGINT NOT NULL,

  priority         INTEGER NOT NULL DEFAULT 100, -- lower = sooner
  requested_by     TEXT NOT NULL,
  requested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  status           TEXT NOT NULL DEFAULT 'queued',  -- 'queued' | 'running' (active only)
  attempts         INTEGER NOT NULL DEFAULT 0,
  last_error       TEXT,
  worker_id        TEXT,                         -- advisory: who picked it
  locked_at        TIMESTAMPTZ,                  -- FOR UPDATE SKIP LOCKED timestamp
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,

  params           JSONB NOT NULL DEFAULT '{}'::jsonb, -- watermark overrides, dry-run, etc.
  
  -- Databricks workflow tracking
  databricks_run_id  TEXT,
  databricks_run_url TEXT
);

CREATE INDEX triggers_ready_idx ON control.triggers (status, priority);

-- 7) Validation history (completed validations, archived after 30 days)
CREATE TABLE control.validation_history (
  id                BIGSERIAL PRIMARY KEY,
  
  -- Link back to original trigger (before it was deleted)
  trigger_id        BIGINT NOT NULL,
  entity_type       TEXT NOT NULL,  -- 'table' | 'compare_query'
  entity_id         BIGINT NOT NULL,
  entity_name       TEXT NOT NULL,  -- Denormalized for easier display
  
  -- Execution metadata
  source           TEXT NOT NULL,     -- 'manual' | 'schedule' | 'bulk_job'
  schedule_id      BIGINT REFERENCES control.schedules(id),
  requested_by     TEXT NOT NULL,
  requested_at     TIMESTAMPTZ NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL,
  finished_at      TIMESTAMPTZ NOT NULL,
  duration_seconds INTEGER GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finished_at - started_at))::INTEGER) STORED,
  
  -- Systems (denormalized for easier querying after systems are deleted/changed)
  source_system_id  BIGINT NOT NULL,
  target_system_id  BIGINT NOT NULL,
  source_system_name TEXT NOT NULL,
  target_system_name TEXT NOT NULL,
  
  -- Validation configuration (what was run)
  source_table      TEXT,  -- For tables (schema.table format)
  target_table      TEXT,  -- For tables (schema.table format)
  sql_query         TEXT,  -- For queries
  compare_mode      TEXT NOT NULL,
  pk_columns        TEXT[],
  exclude_columns   TEXT[],
  
  -- Validation results
  status            TEXT NOT NULL,  -- 'succeeded' | 'failed' | 'canceled'
  
  -- Schema validation
  schema_match      BOOLEAN,
  schema_details    JSONB,  -- {columns_matched: [...], columns_missing: [...], columns_extra: [...]}
  
  -- Row count validation
  row_count_source  BIGINT,
  row_count_target  BIGINT,
  row_count_match   BOOLEAN,
  
  -- Row-level validation (only if row counts match)
  rows_compared     BIGINT,
  rows_matched      BIGINT,
  rows_different    BIGINT,
  difference_pct    NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE WHEN rows_compared > 0 
    THEN (rows_different::NUMERIC / rows_compared * 100)::NUMERIC(5,2)
    ELSE 0 END
  ) STORED,
  
  -- Sample differences (limited for UI display)
  sample_differences JSONB,  -- [{row_key: ..., column: ..., src_val: ..., tgt_val: ...}, ...] (max 100)
  
  -- Error handling
  error_message     TEXT,
  error_details     JSONB,
  
  -- Databricks tracking
  databricks_run_id TEXT NOT NULL,
  databricks_run_url TEXT,
  
  -- Full raw result (for debugging)
  full_result       JSONB,
  
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for common queries
CREATE INDEX validation_history_entity_idx ON control.validation_history (entity_type, entity_id);
CREATE INDEX validation_history_time_idx ON control.validation_history (finished_at DESC);
CREATE INDEX validation_history_status_idx ON control.validation_history (status);
CREATE INDEX validation_history_schedule_idx ON control.validation_history (schedule_id) WHERE schedule_id IS NOT NULL;
CREATE INDEX validation_history_created_at_idx ON control.validation_history (created_at DESC);

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

-- 9) Global validation configuration (singleton table)
CREATE TABLE control.validation_config (
  id                      INTEGER PRIMARY KEY DEFAULT 1,
  downgrade_unicode       BOOLEAN NOT NULL DEFAULT FALSE,
  replace_special_char    TEXT[] NOT NULL DEFAULT '{}',
  extra_replace_regex     TEXT NOT NULL DEFAULT E'\\\\.\\\\.\\\\.',
  updated_by              TEXT NOT NULL DEFAULT 'system',
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT only_one_row CHECK (id = 1)
);

-- Insert default config
INSERT INTO control.validation_config (id, downgrade_unicode, replace_special_char, extra_replace_regex, updated_by)
VALUES (1, FALSE, ARRAY['7F','?'], E'\\\\.\\\\.\\\\.',  'system')
ON CONFLICT (id) DO NOTHING;

-- 10) Type transformations for cross-system validations
CREATE TABLE control.type_transformations (
  id                BIGSERIAL PRIMARY KEY,
  system_a_id       BIGINT NOT NULL REFERENCES control.systems(id) ON DELETE CASCADE,
  system_b_id       BIGINT NOT NULL REFERENCES control.systems(id) ON DELETE CASCADE,
  system_a_function TEXT NOT NULL,  -- Python function for system A
  system_b_function TEXT NOT NULL,  -- Python function for system B
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by        TEXT NOT NULL,
  version           INTEGER NOT NULL DEFAULT 1,
  
  -- Prevent self-referential pairs
  CONSTRAINT different_systems CHECK (system_a_id != system_b_id)
);

-- Ensure non-directional uniqueness: (1,2) = (2,1)
-- Using CREATE UNIQUE INDEX instead of inline constraint for compatibility
CREATE UNIQUE INDEX unique_system_pair ON control.type_transformations (
  LEAST(system_a_id, system_b_id), 
  GREATEST(system_a_id, system_b_id)
);

CREATE INDEX type_transformations_system_a_idx ON control.type_transformations (system_a_id);
CREATE INDEX type_transformations_system_b_idx ON control.type_transformations (system_b_id);
