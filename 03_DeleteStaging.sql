-- FILE: delete_staging_for.sql
BEGIN;
CREATE OR REPLACE FUNCTION engine.delete_staging_for(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM engine.staging_historical
  WHERE forecast_id = p_forecast_id;
END;
$$;
COMMIT;
