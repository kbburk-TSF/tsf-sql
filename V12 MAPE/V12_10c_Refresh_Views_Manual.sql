-- V12_10c_Refresh_Views_Manual.sql
-- Date: 2025-09-24
-- CHANGE LOG:
--   • Explicit DROP outside of a transaction; then CREATE with p_forecast_name.
--   • Avoids rollback blocking the DROP in the same transaction.

DROP FUNCTION IF EXISTS engine.refresh_views_for_forecast(text);

CREATE OR REPLACE FUNCTION engine.refresh_views_for_forecast(p_forecast_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  v_id uuid;
BEGIN
  -- Resolve forecast_id for provided name
  SELECT fr.forecast_id
    INTO v_id
    FROM engine.forecast_registry fr
   WHERE fr.forecast_name = p_forecast_name
   ORDER BY fr.created_at DESC
   LIMIT 1;

  IF v_id IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found for forecast_name=%', p_forecast_name;
  END IF;

  -- Call the same pre-bake refresher used by wrappers
  PERFORM engine.refresh_all_prebaked_views(v_id);
END;
$$;

GRANT EXECUTE ON FUNCTION engine.refresh_views_for_forecast(text) TO aq_engine_owner, tsf_engine_app, matrix_reader;
