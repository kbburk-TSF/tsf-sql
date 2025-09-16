-- =====================================================================
-- FILE: V3_02a_Create_Forecast_Instance_Tables.sql
-- PURPOSE: For EVERY seasonal model SR table (engine.<table>_instance_sr_s),
--          create THREE forecast instance tables with IDENTICAL schema:
--            • engine.<table>_instance_forecast_ms
--            • engine.<table>_instance_forecast_msq
--            • engine.<table>_instance_forecast_msqm
--          Column names updated: "<table>" → series, "<table>_yqm" → season.
-- NOTES:
--   • Pure DDL utility. No business-logic changes.
--   • Types for series and season are inferred from SR table columns
--     <table> and <table>_yqm (sources unchanged).
--   • Idempotent: CREATE TABLE IF NOT EXISTS and created_at safeguard.
-- =====================================================================
-- VC 2025-09-13 v1.0: initial creation utility.
-- VC 2025-09-16 v2.0: CHANGE — rename output column names to series/season;
--                     CHANGE — create ms, msq, msqm tables (same schema).

DO $$
DECLARE
  r            record;
  base         text;        -- <table> name, e.g., me_s_mr30
  sr_rel       text;        -- e.g., me_s_mr30_instance_sr_s
  series_type  text;        -- SQL type of SR "<table>" column
  yqm_type     text;        -- SQL type of SR "<table>_yqm" column
  dest_rel     text;
  dest_qual    text;
  suffix       text;
BEGIN
  PERFORM set_config('client_min_messages','NOTICE',true);
  RAISE NOTICE '[%] START create *_instance_forecast_ms[ q | qm ] tables', clock_timestamp();

  -- Loop all SR tables
  FOR r IN
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'engine'
      AND tablename LIKE '%\_instance\_sr\_s' ESCAPE '\'
    ORDER BY tablename
  LOOP
    sr_rel := r.tablename;
    base   := regexp_replace(sr_rel, '_instance_sr_s$', '');  -- <table>

    -- Derive types from SR source columns (<table>, <table>_yqm)
    SELECT format_type(a.atttypid, a.atttypmod)
      INTO series_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'engine'
      AND c.relname = sr_rel
      AND a.attname = base
      AND a.attnum > 0
      AND NOT a.attisdropped;

    SELECT format_type(a.atttypid, a.atttypmod)
      INTO yqm_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'engine'
      AND c.relname = sr_rel
      AND a.attname = (base || '_yqm')
      AND a.attnum > 0
      AND NOT a.attisdropped;

    IF series_type IS NULL OR yqm_type IS NULL THEN
      RAISE NOTICE '[%] SKIP % — cannot infer types for % or %_yqm from %',
                   clock_timestamp(), base, base, base, sr_rel;
      CONTINUE;
    END IF;

    -- Create three sibling tables with identical schema
    FOREACH suffix IN ARRAY ARRAY[
      '_instance_forecast_ms',
      '_instance_forecast_msq',
      '_instance_forecast_msqm'
    ]
    LOOP
      dest_rel  := base || suffix;
      dest_qual := format('%I.%I', 'engine', dest_rel);

      RAISE NOTICE '[%] CREATE % (series %, season %)', clock_timestamp(), dest_qual, series_type, yqm_type;

      EXECUTE format($DDL$
        CREATE TABLE IF NOT EXISTS %s (
          forecast_id           uuid NOT NULL,
          "date"                date NOT NULL,
          value                 numeric,
          series                %s,           -- renamed from "<table>"
          season                %s,           -- renamed from "<table>_yqm"
          model_name            text,
          base_model            text,
          base_fv               numeric,
          fmsr_series           text,
          fmsr_value            numeric,
          fv                    numeric,
          fv_error              numeric,
          fv_mae                numeric,
          fv_mean_mae           numeric,
          fv_mean_mae_c         numeric,
          fv_u                  numeric,
          fv_l                  numeric,
          mae_comparison        text,
          mean_mae_comparison   text,
          accuracy_comparison   text,
          best_fm_count         integer,
          best_fm_odds          numeric,
          best_fm_sig           numeric,
          fv_interval           text,
          fv_interval_c         numeric,
          fv_interval_odds      numeric,
          fv_interval_sig       numeric,
          fv_variance           numeric,
          fv_variance_mean      numeric,
          created_at            timestamptz DEFAULT now()
        )
      $DDL$, dest_qual, series_type, yqm_type);

      -- Ensure created_at exists on preexisting tables
      EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()', dest_qual);

      RAISE NOTICE '[%] READY %', clock_timestamp(), dest_qual;
    END LOOP;
  END LOOP;

  RAISE NOTICE '[%] ALL DONE', clock_timestamp();
END
$$ LANGUAGE plpgsql;
