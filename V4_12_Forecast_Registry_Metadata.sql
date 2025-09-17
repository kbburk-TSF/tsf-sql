-- V4_12_Forecast_Registry_Metadata.sql
-- Register/maintain forecast metadata in engine.forecast_registry (no triggers).
-- VC V4.0 (2025-09-17): New helper to upsert forecast_id, forecast_name, source filename/URI, and parameters.
-- Usage: call BEFORE loading staging_historical for the same forecast_id.
-- Notes: insert status='created' here; your existing staging trigger will flip it to 'staged' on upload.

BEGIN;

CREATE SCHEMA IF NOT EXISTS engine;

CREATE OR REPLACE FUNCTION engine.register_forecast(
  p_forecast_id          uuid,
  p_forecast_name        text,
  p_source_csv_filename  text DEFAULT NULL,
  p_classical_source_uri text DEFAULT NULL,
  p_parameters           jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  -- Upsert metadata; do not force status if row already exists
  INSERT INTO engine.forecast_registry(
    forecast_id, forecast_name, source_csv_filename, classical_source_uri, parameters, status, created_at, updated_at
  )
  VALUES (
    p_forecast_id,
    p_forecast_name,
    p_source_csv_filename,
    p_classical_source_uri,
    COALESCE(p_parameters, '{}'::jsonb),
    'created',
    now(),
    now()
  )
  ON CONFLICT (forecast_id) DO UPDATE
  SET forecast_name        = EXCLUDED.forecast_name,
      source_csv_filename  = COALESCE(EXCLUDED.source_csv_filename, engine.forecast_registry.source_csv_filename),
      classical_source_uri = COALESCE(EXCLUDED.classical_source_uri, engine.forecast_registry.classical_source_uri),
      parameters           = COALESCE(EXCLUDED.parameters, engine.forecast_registry.parameters),
      updated_at           = now();

  -- Ensure totals are present (default 54/54) without touching completions or status
  UPDATE engine.forecast_registry
  SET sr_total = COALESCE(sr_total, 54),
      fi_total = COALESCE(fi_total, 54)
  WHERE forecast_id = p_forecast_id;
END;
$$;

-- Execution privileges
GRANT EXECUTE ON FUNCTION engine.register_forecast(uuid, text, text, text, jsonb) TO matrix_reader, tsf_engine_app;

COMMIT;
