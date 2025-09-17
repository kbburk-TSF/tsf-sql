-- V4_05_DeleteStaging.sql
-- engine.delete_staging_for(): post-success cleanup of staging rows for a forecast_id
-- VC V4.0 (2025-09-17): Carried forward from V3_05 with no logic changes; aligned grants to matrix_reader/tsf_engine_app.

BEGIN;

CREATE SCHEMA IF NOT EXISTS engine;

CREATE OR REPLACE FUNCTION engine.delete_staging_for(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  -- best-effort session tuning (ignore errors if not allowed)
  BEGIN
    PERFORM set_config('client_min_messages','NOTICE',true);
    PERFORM set_config('jit','off',true);
    PERFORM set_config('synchronous_commit','off',true);
  EXCEPTION WHEN OTHERS THEN
    -- do nothing
  END;

  RAISE NOTICE '[%] delete_staging_for start', clock_timestamp();

  DELETE FROM engine.staging_historical
  WHERE forecast_id = p_forecast_id;

  RAISE NOTICE '[%] delete_staging_for done', clock_timestamp();
END;
$$;

-- Execution privileges
GRANT EXECUTE ON FUNCTION engine.delete_staging_for(uuid) TO matrix_reader;
GRANT EXECUTE ON FUNCTION engine.delete_staging_for(uuid) TO tsf_engine_app;

COMMIT;
