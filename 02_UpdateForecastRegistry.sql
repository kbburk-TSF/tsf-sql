-- FILE: update_forecast_registry.sql
BEGIN;
CREATE OR REPLACE FUNCTION engine.update_forecast_registry(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Update existing row if present
  UPDATE engine.forecast_registry fr
  SET
    forecast_name = COALESCE(
      (SELECT MAX(sh.forecast_name)
         FROM engine.staging_historical sh
        WHERE sh.forecast_id = p_forecast_id),
      fr.forecast_name
    ),
    status = 'loading'
  WHERE fr.forecast_id = p_forecast_id;

  -- If nothing was updated, insert a new row
  IF NOT FOUND THEN
    INSERT INTO engine.forecast_registry
      (forecast_id, forecast_name, source_csv_filename, parameters, status)
    VALUES
      (
        p_forecast_id,
        (SELECT MAX(sh.forecast_name)
           FROM engine.staging_historical sh
          WHERE sh.forecast_id = p_forecast_id),
        '(unknown)',
        '{}'::jsonb,
        'loading'
      );
  END IF;
END;
$$;
COMMIT;
