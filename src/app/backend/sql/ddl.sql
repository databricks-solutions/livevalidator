-- 1) Connection targets / engines
CREATE TABLE IF NOT EXISTS control.systems (
  id                BIGSERIAL,
  name              TEXT NOT NULL UNIQUE,
  kind              TEXT NOT NULL,
  catalog           TEXT,
  host              TEXT,
  port              INTEGER,
  database          TEXT,
  secret_scope      TEXT DEFAULT 'livevalidator',
  user_secret_key   TEXT,
  pass_secret_key   TEXT,
  jdbc_string       TEXT,
  driver_connector  TEXT,                          -- Custom JDBC driver class or Spark connector
  concurrency       INTEGER NOT NULL DEFAULT -1,
  max_rows          INTEGER DEFAULT NULL,              -- Max rows to pull during validation (NULL = unlimited)
  options           JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  created_by        TEXT NOT NULL,
  updated_by        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  version           INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (id)
);

-- 2) Named table ↔ table comparisons (schema-driven)
CREATE TABLE IF NOT EXISTS control.datasets (
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
  pk_vetted        BOOLEAN NOT NULL DEFAULT FALSE,       -- whether PK columns have been validated
  watermark_filter TEXT DEFAULT NULL,                    -- optional WHERE clause filter (e.g., "created_at > '2024-01-01'")
  include_columns  TEXT[] NOT NULL DEFAULT '{}',         -- default: compare all
  exclude_columns  TEXT[] NOT NULL DEFAULT '{}',
  options          JSONB NOT NULL DEFAULT '{}'::jsonb,   -- tolerances, null eq, coercions
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  config_overrides JSONB DEFAULT NULL,                   -- DEPRECATED: use control.config table instead
  lineage          JSONB DEFAULT NULL,                   -- upstream lineage (populated via Databricks Lineage API)

  created_by       TEXT NOT NULL,
  updated_by       TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  version          INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS datasets_src_system_idx ON control.datasets (src_system_id);
CREATE INDEX IF NOT EXISTS datasets_tgt_system_idx ON control.datasets (tgt_system_id);
CREATE INDEX IF NOT EXISTS datasets_active_idx ON control.datasets (name) WHERE is_active;

-- 3) Arbitrary SQL ↔ SQL comparisons
CREATE TABLE IF NOT EXISTS control.compare_queries (
  id               BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,         -- "daily_sales_sql"
  sql              TEXT NOT NULL,
  src_system_id    BIGINT NOT NULL REFERENCES control.systems(id),
  tgt_system_id    BIGINT NOT NULL REFERENCES control.systems(id),

  compare_mode     TEXT NOT NULL DEFAULT 'except_all',   -- same options as datasets
  pk_columns       TEXT[] DEFAULT NULL,
  pk_vetted        BOOLEAN NOT NULL DEFAULT FALSE,       -- whether PK columns have been validated
  watermark_filter TEXT DEFAULT NULL,                    -- optional WHERE clause filter
  options          JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  config_overrides JSONB DEFAULT NULL,                   -- DEPRECATED: use control.config table instead
  lineage          JSONB DEFAULT NULL,                   -- upstream lineage (populated via Databricks Lineage API)

  created_by       TEXT NOT NULL,
  updated_by       TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  version          INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS compare_queries_active_idx ON control.compare_queries (name) WHERE is_active;
CREATE INDEX IF NOT EXISTS compare_queries_src_tgt_idx ON control.compare_queries (src_system_id, tgt_system_id);

-- 4) Schedules (cron-ish) + state
CREATE TABLE IF NOT EXISTS control.schedules (
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
CREATE TABLE IF NOT EXISTS control.schedule_bindings (
  id               BIGSERIAL PRIMARY KEY,
  schedule_id      BIGINT NOT NULL REFERENCES control.schedules(id) ON DELETE CASCADE,
  entity_type      TEXT  NOT NULL,               -- 'table' | 'compare_query'
  entity_id        BIGINT NOT NULL,              -- FK by app logic
  UNIQUE (schedule_id, entity_type, entity_id)
);

-- 6) Triggers / run queue (manual & scheduled). Workers pop SKIP LOCKED.
CREATE TABLE IF NOT EXISTS control.triggers (
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

CREATE INDEX IF NOT EXISTS triggers_ready_idx ON control.triggers (status, priority);

-- 7) Validation history (completed validations, archived after 30 days)
CREATE TABLE IF NOT EXISTS control.validation_history (
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
CREATE INDEX IF NOT EXISTS validation_history_entity_idx ON control.validation_history (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS validation_history_time_idx ON control.validation_history (finished_at DESC);
CREATE INDEX IF NOT EXISTS validation_history_requested_at_idx ON control.validation_history (requested_at DESC);
CREATE INDEX IF NOT EXISTS validation_history_status_idx ON control.validation_history (status);
CREATE INDEX IF NOT EXISTS validation_history_schedule_idx ON control.validation_history (schedule_id) WHERE schedule_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS validation_history_created_at_idx ON control.validation_history (created_at DESC);

-- 8) Tags for organization / filters in UI
CREATE TABLE IF NOT EXISTS control.tags (
  id    BIGSERIAL PRIMARY KEY,
  name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS control.entity_tags (
  entity_type TEXT NOT NULL,
  entity_id   BIGINT NOT NULL,
  tag_id      BIGINT NOT NULL REFERENCES control.tags(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id, tag_id)
);

-- Index for fast tag lookups by entity (used in validation-history query)
CREATE INDEX IF NOT EXISTS entity_tags_entity_idx ON control.entity_tags (entity_type, entity_id);

-- 9) Global validation configuration (legacy singleton table - DEPRECATED)
-- Use control.config table instead for new configurations
CREATE TABLE IF NOT EXISTS control.validation_config (
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

-- 9b) Flexible configuration (scoped: global, table, compare_query)
-- Replaces validation_config and config_overrides columns
CREATE TABLE IF NOT EXISTS control.config (
  id          BIGSERIAL PRIMARY KEY,
  scope       TEXT NOT NULL,           -- 'global', 'table', 'compare_query'
  scope_id    INTEGER,                 -- NULL for global, entity_id for overrides
  settings    JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_by  TEXT,
  updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS config_scope_unique 
  ON control.config (scope, COALESCE(scope_id, -1));

-- 10) Type transformations for cross-system validations
CREATE TABLE IF NOT EXISTS control.type_transformations (
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
CREATE UNIQUE INDEX IF NOT EXISTS unique_system_pair ON control.type_transformations (
  LEAST(system_a_id, system_b_id), 
  GREATEST(system_a_id, system_b_id)
);

CREATE INDEX IF NOT EXISTS type_transformations_system_a_idx ON control.type_transformations (system_a_id);
CREATE INDEX IF NOT EXISTS type_transformations_system_b_idx ON control.type_transformations (system_b_id);

-- User roles for access control
CREATE TABLE IF NOT EXISTS control.user_roles (
    user_email VARCHAR(255) PRIMARY KEY,
    role VARCHAR(20) NOT NULL DEFAULT 'CAN_MANAGE',
    assigned_by VARCHAR(255),
    assigned_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT valid_role CHECK (role IN ('CAN_VIEW', 'CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE'))
);

CREATE INDEX IF NOT EXISTS user_roles_role_idx ON control.user_roles (role);

-- Application configuration (key-value store)
CREATE TABLE IF NOT EXISTS control.app_config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    updated_by VARCHAR(255),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert default configuration values
INSERT INTO control.app_config (key, value, description, updated_by) 
VALUES ('default_user_role', 'CAN_MANAGE', 'Default role assigned to new users on first access', 'system')
ON CONFLICT (key) DO NOTHING;

-- 14) Dashboards (persistent, stateful dashboard configurations)
CREATE TABLE IF NOT EXISTS control.dashboards (
  id                BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  project           TEXT NOT NULL DEFAULT 'General',
  time_range_preset TEXT NOT NULL DEFAULT '7d',
  time_range_from   TIMESTAMPTZ,
  time_range_to     TIMESTAMPTZ,
  created_by        TEXT NOT NULL,
  updated_by        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  version           INTEGER NOT NULL DEFAULT 1,
  UNIQUE (name, created_by)
);

CREATE INDEX IF NOT EXISTS dashboards_created_by_idx ON control.dashboards (created_by);
CREATE INDEX IF NOT EXISTS dashboards_project_idx ON control.dashboards (project);

-- 15) Dashboard charts (per-chart filter configurations within a dashboard)
CREATE TABLE IF NOT EXISTS control.dashboard_charts (
  id               BIGSERIAL PRIMARY KEY,
  dashboard_id     BIGINT NOT NULL REFERENCES control.dashboards(id) ON DELETE CASCADE,
  name             TEXT NOT NULL,
  sort_order       INTEGER NOT NULL DEFAULT 0,
  filters          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dashboard_charts_dashboard_idx ON control.dashboard_charts (dashboard_id);


-- [ Schema migrations ] Add columns introduced after initial release

-- 1) Add lineage column to datasets and compare_queries
ALTER TABLE control.datasets ADD COLUMN IF NOT EXISTS lineage JSONB DEFAULT NULL;
ALTER TABLE control.compare_queries ADD COLUMN IF NOT EXISTS lineage JSONB DEFAULT NULL;

-- 2) Migrate existing config to control.config table
-- Migrate global config from legacy validation_config table
INSERT INTO control.config (scope, scope_id, settings, updated_by, updated_at)
SELECT 'global', NULL,
  jsonb_build_object(
    'downgrade_unicode', downgrade_unicode,
    'replace_special_char', replace_special_char,
    'extra_replace_regex', extra_replace_regex
  ),
  updated_by, updated_at
FROM control.validation_config WHERE id = 1
ON CONFLICT (scope, COALESCE(scope_id, -1)) DO NOTHING;

-- Seed empty global config if no migration occurred
INSERT INTO control.config (scope, scope_id, settings, updated_by)
VALUES ('global', NULL, '{}'::jsonb, 'system')
ON CONFLICT (scope, COALESCE(scope_id, -1)) DO NOTHING;

-- Migrate entity-level config_overrides to control.config (datasets)
INSERT INTO control.config (scope, scope_id, settings, updated_by, updated_at)
SELECT 'table', id, config_overrides, updated_by, updated_at
FROM control.datasets 
WHERE config_overrides IS NOT NULL AND config_overrides != '{}'::jsonb
ON CONFLICT (scope, COALESCE(scope_id, -1)) DO NOTHING;

-- Migrate entity-level config_overrides to control.config (compare_queries)
INSERT INTO control.config (scope, scope_id, settings, updated_by, updated_at)
SELECT 'compare_query', id, config_overrides, updated_by, updated_at
FROM control.compare_queries 
WHERE config_overrides IS NOT NULL AND config_overrides != '{}'::jsonb
ON CONFLICT (scope, COALESCE(scope_id, -1)) DO NOTHING;

-- 3) Add pk_vetted column to datasets and compare_queries
ALTER TABLE control.datasets ADD COLUMN IF NOT EXISTS pk_vetted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE control.compare_queries ADD COLUMN IF NOT EXISTS pk_vetted BOOLEAN NOT NULL DEFAULT FALSE;
