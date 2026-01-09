-- Drop all tables in control schema dynamically (CASCADE will drop dependent objects)
-- This does NOT drop the schema itself, avoiding permission issues
-- Automatically finds and drops all tables, no need to maintain explicit list

DO $$ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'control') 
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS control.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
