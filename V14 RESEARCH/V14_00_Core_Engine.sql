-- V14_00_Core_Engine.sql
-- Target DB: tsf_research_dev
-- Run this after connecting to the tsf_research_dev database.
-- No placeholders. No role switching. Ownership ties to whoever runs it.

BEGIN;

-- 1) Schema: create if missing; make CURRENT_USER the owner; restrict PUBLIC; expose to runtime roles
CREATE SCHEMA IF NOT EXISTS engine AUTHORIZATION CURRENT_USER;
ALTER SCHEMA engine OWNER TO CURRENT_USER;

REVOKE ALL ON SCHEMA engine FROM PUBLIC;
GRANT USAGE ON SCHEMA engine TO aq_engine_owner, tsf_engine_app, matrix_reader;

-- 2) Default privileges for FUTURE objects created by the current user in schema engine
--    Tables
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  REVOKE SELECT ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT ALL PRIVILEGES ON TABLES TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT SELECT ON TABLES TO tsf_engine_app, matrix_reader;

--    Sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT ALL PRIVILEGES ON SEQUENCES TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT USAGE, SELECT ON SEQUENCES TO tsf_engine_app, matrix_reader;

--    Functions
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT EXECUTE ON FUNCTIONS TO aq_engine_owner, tsf_engine_app, matrix_reader;

--    Types
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT USAGE ON TYPES TO aq_engine_owner, tsf_engine_app, matrix_reader;

-- 3) Bring EXISTING objects (if any) into compliance (safe to re-run)
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA engine TO aq_engine_owner;
GRANT SELECT         ON ALL TABLES    IN SCHEMA engine TO tsf_engine_app, matrix_reader;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA engine TO aq_engine_owner;
GRANT USAGE, SELECT  ON ALL SEQUENCES IN SCHEMA engine TO tsf_engine_app, matrix_reader;

GRANT EXECUTE        ON ALL FUNCTIONS IN SCHEMA engine TO aq_engine_owner, tsf_engine_app, matrix_reader;

-- 4) Required extension used by downstream scripts
CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;

COMMIT;
