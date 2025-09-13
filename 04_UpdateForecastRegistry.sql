-- FILE: update_forecast_registry.sql
-- Version: 2025-09-13  v2.0
-- Change: OPT — session tuning (best-effort) + progress notices; no logic changes.
BEGIN;
CREATE OR REPLACE FUNCTION engine.update_forecast_registry(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  BEGIN
    PERFORM set_config('client_min_messages','NOTICE',true);
    PERFORM set_config('jit','off',true);
    PERFORM set_config('synchronous_commit','off',true);
  EXCEPTION WHEN OTHERS THEN END;
  RAISE NOTICE '[%] update_forecast_registry start', clock_timestamp();

  -- Update existing row if present
  UPDATE engine.forecast_registry fr
  SET
    forecast_name = COALESCE(
      (SELECT MAX(sh.forecast_name)
         FROM engine.staging_historical sh
        WHERE sh.forecast_id = p_forecast_id),
      fr.forecast_name
    ),
    meta = COALESCE(fr.meta, '{}'::jsonb),
    status = 'loaded',
    updated_at = now()
  WHERE fr.forecast_id = p_forecast_id;

  IF NOT FOUND THEN
    INSERT INTO engine.forecast_registry (forecast_id, forecast_name, owner, meta, status)
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

  RAISE NOTICE '[%] update_forecast_registry done', clock_timestamp();
END;
$$;
COMMIT;
-- FILE: 04_UpdateForecastRegistry.sql
-- PURPOSE: Upsert the forecast_registry row after staging ingest
-- VC 2025-09-13: Fix to use parameters (jsonb) instead of non-existent meta;
--                update updated_at only when the column exists. No other changes.

BEGIN;

CREATE OR REPLACE FUNCTION engine.update_forecast_registry(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  has_updated_at boolean;
BEGIN
  IF p_forecast_id IS NULL THEN
    RAISE EXCEPTION 'update_forecast_registry: p_forecast_id is required';
  END IF;

  -- Detect optional updated_at column so this works across schema variations.
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'engine'
      AND table_name   = 'forecast_registry'
      AND column_name  = 'updated_at'
  ) INTO has_updated_at;

  -- Try to UPDATE first.
  IF has_updated_at THEN
    EXECUTE $sql$
      UPDATE engine.forecast_registry fr
         SET forecast_name = COALESCE(
               (SELECT MAX(sh.forecast_name)
                  FROM engine.staging_historical sh
                 WHERE sh.forecast_id = $1),
               fr.forecast_name
             ),
             parameters    = COALESCE(fr.parameters, '{}'::jsonb),
             status        = 'loaded',
             updated_at    = now()
       WHERE fr.forecast_id = $1
    $sql$ USING p_forecast_id;
  ELSE
    EXECUTE $sql$
      UPDATE engine.forecast_registry fr
         SET forecast_name = COALESCE(
               (SELECT MAX(sh.forecast_name)
                  FROM engine.staging_historical sh
                 WHERE sh.forecast_id = $1),
               fr.forecast_name
             ),
             parameters    = COALESCE(fr.parameters, '{}'::jsonb),
             status        = 'loaded'
       WHERE fr.forecast_id = $1
    $sql$ USING p_forecast_id;
  END IF;

  -- If no row was updated, INSERT one.
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
        'loaded'
      );
  END IF;
END;
$$;

COMMIT;
