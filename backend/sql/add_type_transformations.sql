-- Add type_transformations table to existing schema
-- Run this file to add the new table for Type Mappings feature

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
  
  -- Ensure non-directional uniqueness: (1,2) = (2,1)
  CONSTRAINT unique_system_pair UNIQUE (
    LEAST(system_a_id, system_b_id), 
    GREATEST(system_a_id, system_b_id)
  ),
  -- Prevent self-referential pairs
  CONSTRAINT different_systems CHECK (system_a_id != system_b_id)
);

CREATE INDEX IF NOT EXISTS type_transformations_system_a_idx ON control.type_transformations (system_a_id);
CREATE INDEX IF NOT EXISTS type_transformations_system_b_idx ON control.type_transformations (system_b_id);

-- Success message
SELECT 'Type transformations table created successfully!' as status;

