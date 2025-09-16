-- 01a_Set_Up_Foreign_Server_Access.sql
-- Create seasonal-matrix FDW server, user mappings, import designated models into engine schema,
-- and grant visibility so foreign tables appear in Neon + TablePlus.
-- VC 3.0 (2025-09-15): Single-pass setup; PUBLIC removed; grants to neondb_owner + tsf_engine_app (+ engine_reader).

BEGIN;

-- 0) Prereqs
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 1) Foreign server (update host/dbname if your seasonal-matrix endpoint changes)
CREATE SERVER IF NOT EXISTS seasonal_matrix_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host    'ep-winter-dew-adnmg5hw-pooler.c-2.us-east-1.aws.neon.tech',
    dbname  'neondb',
    port    '5432',
    sslmode 'require'
  );

-- 2) Allow roles to use this server
GRANT USAGE ON FOREIGN SERVER seasonal_matrix_srv TO neondb_owner;
GRANT USAGE ON FOREIGN SERVER seasonal_matrix_srv TO tsf_engine_app;

-- 3) User mappings (maps local roles to the remote seasonal-matrix credentials)
--    (Update password if it rotated)
CREATE USER MAPPING IF NOT EXISTS
FOR neondb_owner
SERVER seasonal_matrix_srv
OPTIONS (
  user 'neondb_owner',
  password 'npg_YlT60kjRFICA'
);

CREATE USER MAPPING IF NOT EXISTS
FOR tsf_engine_app
SERVER seasonal_matrix_srv
OPTIONS (
  user 'neondb_owner',
  password 'npg_YlT60kjRFICA'
);

-- 4) Ensure target schema exists locally (no-op if already created earlier)
CREATE SCHEMA IF NOT EXISTS engine;

-- 5) Import designated seasonal models from the remote schema into local engine schema
--    (Add more LIMIT TO entries as needed)
IMPORT FOREIGN SCHEMA seasonal_matrix
  LIMIT TO (me_s_mr30)
  FROM SERVER seasonal_matrix_srv
  INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
  LIMIT TO (mo_s)
  FROM SERVER seasonal_matrix_srv
  INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
  LIMIT TO (wd_md)
  FROM SERVER seasonal_matrix_srv
  INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
  LIMIT TO (mo_sf)
  FROM SERVER seasonal_matrix_srv
  INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
  LIMIT TO (me_mr10)
  FROM SERVER seasonal_matrix_srv
  INTO engine;

-- 6) Visibility & access (NO PUBLIC). These make the foreign tables show up in Neon/TablePlus.
--    Grant USAGE on schema and SELECT on the imported foreign tables to your working roles.
REVOKE ALL ON SCHEMA engine FROM PUBLIC;
GRANT  USAGE ON SCHEMA engine TO neondb_owner, tsf_engine_app, engine_reader;

-- Grant SELECT on all (includes newly imported foreign tables)
GRANT SELECT ON ALL TABLES IN SCHEMA engine TO neondb_owner, tsf_engine_app, engine_reader;

-- Also set defaults so future imports are visible without re-granting (applies to the current role running this script)
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT SELECT ON TABLES TO neondb_owner, tsf_engine_app, engine_reader;

COMMIT;
