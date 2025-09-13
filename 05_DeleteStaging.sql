-- FILE: delete_staging_for.sql
-- Version: 2025-09-13  v2.0
-- Change: OPT — session tuning (best-effort) + progress notices; no logic changes.
BEGIN;
CREATE OR REPLACE FUNCTION engine.delete_staging_for(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  BEGIN
    PERFORM set_config('client_min_messages','NOTICE',true);
    PERFORM set_config('jit','off',true);
    PERFORM set_config('synchronous_commit','off',true);
  EXCEPTION WHEN OTHERS THEN END;
  RAISE NOTICE '[%] delete_staging_for start', clock_timestamp();

  DELETE FROM engine.staging_historical
  WHERE forecast_id = p_forecast_id;

  RAISE NOTICE '[%] delete_staging_for done', clock_timestamp();
END;
$$;
COMMIT;
