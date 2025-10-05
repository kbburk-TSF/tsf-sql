-- V14_10_MSQM_FORECAST.sql
-- 2025-10-04 V14_10: New build from V13_09c per CANNON (2025-10-04).
-- KEEP wrapper & signatures EXACT. Update ONLY core logic + destination schema to match CANNON.

-- 2025-10-04 HOTFIX-13: Guard __tmp_forecast_build creation; if build fails, create empty LIKE __work so downstream steps don't crash. No other changes.
-- 2025-10-04 HOTFIX-14: After attempted temp-build, unconditionally ensure __tmp_forecast_build exists via LIKE dest_qual; guarantees presence before downstream use. No wrapper/signature changes.
-- 2025-10-04 HOTFIX-15: Add missing 'EXECUTE sql;' after temp-build format block so __tmp_forecast_build is actually created. No wrapper/signature changes.
-- 2025-10-04 V14_10: Rebuilt core per Optimization Plan; fixed temp build: EXECUTE sql outside string; DELETE/UPDATE/ANALYZE after creation; no binomial.
CREATE OR REPLACE FUNCTION engine.ms_forecast(forecast_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  PERFORM set_config('engine.forecast_name', forecast_name, true);
  PERFORM engine.ms_forecast__core();
  PERFORM set_config('engine.forecast_name', '', true);
END;
$$;

CREATE OR REPLACE FUNCTION engine.ms_forecast__core()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
v_forecast_name text;
  forecast_target_id uuid;
enable_extended_stats   boolean := false;
  enable_cluster_vacuum   boolean := false;
  enable_full_analyze     boolean := true;
  enable_binom_build      boolean := true;
  t_run_start   timestamptz := clock_timestamp();
  t_series_start timestamptz;
  t_pass_start   timestamptz;
  r                 record;
  base              text;
  sr_rel            text;
  sr_qual           text;
  latest_id         uuid;
  start_from        date;
  dest_rel          text;
  dest_qual         text;
  dest_real_qual   text;
dest_series_col   text;
  dest_season_col   text;
  sr_base_col       text;
  sr_yqm_col        text;
  sr_fmsr_a1_col    text;
  sr_fmsr_a2_col    text;
  sr_fmsr_a2w_col   text;
  sr_fmsr_a3_col    text;
  sr_fmsr_a3w_col   text;
  h_lmm1            text := 'h.'||quote_ident('lmm1');
  h_lmm5            text := 'h.'||quote_ident('lmm5');
  h_lmm10           text := 'h.'||quote_ident('lmm10');
  h_lmm15           text := 'h.'||quote_ident('lmm15');
  h_lmm30           text := 'h.'||quote_ident('lmm30');
  h_arima_m         text := 'h.'||quote_ident('arima_m');
  h_ses_m           text := 'h.'||quote_ident('ses_m');
  h_hwes_m          text := 'h.'||quote_ident('hwes_m');
  sql               text;
  rcnt              bigint;
  tname             text;

BEGIN
  v_forecast_name := current_setting('engine.forecast_name', true);
  IF v_forecast_name IS NULL THEN
    RAISE EXCEPTION 'engine.ms_forecast() requires forecast_name';
  END IF;
  SELECT fr.forecast_id INTO forecast_target_id
    FROM engine.forecast_registry fr
   WHERE fr.forecast_name = v_forecast_name
   ORDER BY fr.created_at DESC
   LIMIT 1;
  IF forecast_target_id IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found for forecast_name=%', v_forecast_name;
  END IF;
  latest_id := forecast_target_id;

  CREATE TEMP TABLE __ih_subset ON COMMIT DROP AS
  SELECT * FROM engine.instance_historical WHERE forecast_id = latest_id;
  CREATE INDEX ON __ih_subset (forecast_id, date);

  IF to_regclass('__universe') IS NOT NULL THEN
    EXECUTE 'DROP TABLE IF EXISTS __universe';
  END IF;
  CREATE TEMP TABLE __universe ON COMMIT DROP AS
  SELECT forecast_id, date FROM __ih_subset;
  CREATE INDEX ON __universe (forecast_id, date);
  ANALYZE __universe;

  PERFORM set_config('jit','off',true);
  PERFORM set_config('work_mem','256MB',true);
  PERFORM set_config('maintenance_work_mem','512MB',true);

  RAISE NOTICE 'RUN START';

  FOR r IN
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'engine'
      AND tablename LIKE '%\_instance\_sr\_s' ESCAPE '\'
    ORDER BY tablename
  LOOP
    t_series_start := clock_timestamp();
    sr_rel  := r.tablename;
    base    := regexp_replace(sr_rel, '_instance_sr_s$', '');

    RAISE NOTICE 'series start forecast=% sr_rel=% base=% at=%',
      v_forecast_name, sr_rel, base, clock_timestamp();
    RAISE NOTICE 'forecast=% sr_rel=% base=% start_at=%',
      v_forecast_name, sr_rel, base, clock_timestamp();
    sr_qual := format('%I.%I', 'engine', sr_rel);

    EXECUTE $q$
      SELECT (min(date) + interval '2 years')::date
      FROM __ih_subset
      WHERE forecast_id = $1
    $q$ USING latest_id INTO start_from;
    IF start_from IS NULL THEN
      RAISE NOTICE 'SKIP series % — no historical', base;
      CONTINUE;
    END IF;

    dest_rel  := base || '_instance_forecast_ms';
    dest_qual := format('%I.%I', 'engine', dest_rel);

    IF to_regclass(dest_qual) IS NULL THEN
      EXECUTE format($ct$
        CREATE TABLE %s (
          forecast_id uuid NOT NULL,
          date date NOT NULL,
          value numeric(18,4),
          series text,
          season text,
          model_name text,
          base_model text,
          base_fv numeric,
          fmsr_series text,
          fmsr_value numeric,
          fv numeric,
          fv_error numeric,
          fv_mape numeric,
          fv_mean_mape numeric,
          fv_mean_mape_c numeric,
          fv_mae numeric,
          fv_mean_mae numeric,
          fv_mean_mae_c numeric,
          fv_rmse numeric,
          fv_mean_rmse numeric,
          fv_mean_rmse_c numeric,
          mape_comparison text,
          mean_mape_comparison text,
          accuracy_comparison text,
          best_mape_count integer,
          mae_comparison text,
          mean_mae_comparison text,
          mae_accuracy_comparison text,
          best_mae_count integer,
          rmse_comparison text,
          mean_rmse_comparison text,
          rmse_accuracy_comparison text,
          best_rmse_count integer,
          fv_b125_u numeric, fv_b125_l numeric,
          fv_b150_u numeric, fv_b150_l numeric,
          fv_b175_u numeric, fv_b175_l numeric,
          fv_b200_u numeric, fv_b200_l numeric,
          fv_b225_u numeric, fv_b225_l numeric,
          fv_b250_u numeric, fv_b250_l numeric,
          fv_b275_u numeric, fv_b275_l numeric,
          fv_b300_u numeric, fv_b300_l numeric,
          fv_b325_u numeric, fv_b325_l numeric,
          fv_b350_u numeric, fv_b350_l numeric,
          b125_hit text,
          b150_hit text,
          b175_hit text,
          b200_hit text,
          b225_hit text,
          b250_hit text,
          b275_hit text,
          b300_hit text,
          b325_hit text,
          b350_hit text,
          b125_cov numeric, b125_cov_c numeric,
          b150_cov numeric, b150_cov_c numeric,
          b175_cov numeric, b175_cov_c numeric,
          b200_cov numeric, b200_cov_c numeric,
          b225_cov numeric, b225_cov_c numeric,
          b250_cov numeric, b250_cov_c numeric,
          b275_cov numeric, b275_cov_c numeric,
          b300_cov numeric, b300_cov_c numeric,
          b325_cov numeric, b325_cov_c numeric,
          b350_cov numeric, b350_cov_c numeric,
          ci85_low numeric, ci85_high numeric,
          ci90_low numeric, ci90_high numeric,
          ci95_low numeric, ci95_high numeric,
          fv_variance numeric,
          fv_variance_mean numeric,
          qm_msr numeric,
          msr_dir text,
          fmsr_dir text,
          dir_hit text,
          dir_hit_count integer,
          created_at timestamptz DEFAULT now(),
          PRIMARY KEY (forecast_id, date, model_name, fmsr_series)
        )
      $ct$, dest_qual);
    END IF;

    EXECUTE 'DROP TABLE IF EXISTS __work';
    EXECUTE 'CREATE TEMP TABLE __work (LIKE ' || dest_qual || ' INCLUDING ALL)';
    EXECUTE 'CREATE INDEX ON __work (forecast_id, date)';
    EXECUTE 'ANALYZE __work';
    dest_real_qual := dest_qual;
    dest_qual := '__work';

    dest_series_col := 'series';
    dest_season_col := 'season';
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_series_col;
    IF NOT FOUND THEN dest_series_col := base; END IF;
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_season_col;
    IF NOT FOUND THEN dest_season_col := base || '_yqm'; END IF;

    sr_base_col := 'sr.' || quote_ident(base);
    sr_yqm_col := 'sr.' || quote_ident(base || '_yqm');
    sr_fmsr_a1_col := 'sr.' || quote_ident(base || '_fmsr_a1');
    sr_fmsr_a2_col := 'sr.' || quote_ident(base || '_fmsr_a2');
    sr_fmsr_a2w_col := 'sr.' || quote_ident(base || '_fmsr_a2w');
    sr_fmsr_a3_col := 'sr.' || quote_ident(base || '_fmsr_a3');
    sr_fmsr_a3w_col := 'sr.' || quote_ident(base || '_fmsr_a3w');

    RAISE NOTICE 'PASS 1 — hydrate';
    EXECUTE 'DROP TABLE IF EXISTS __tmp_forecast_build';

    EXECUTE 'DROP TABLE IF EXISTS __tmp_forecast_build';
sql := format($f$

      CREATE TEMP TABLE __tmp_forecast_build AS
      WITH variants AS (
        VALUES
          ('LMM1','lmm1','A0'), ('LMM1','lmm1','A1'), ('LMM1','lmm1','A2'), ('LMM1','lmm1','A2W'), ('LMM1','lmm1','A3'), ('LMM1','lmm1','A3W'),
          ('LMM5','lmm5','A0'), ('LMM5','lmm5','A1'), ('LMM5','lmm5','A2'), ('LMM5','lmm5','A2W'), ('LMM5','lmm5','A3'), ('LMM5','lmm5','A3W'),
          ('LMM10','lmm10','A0'), ('LMM10','lmm10','A1'), ('LMM10','lmm10','A2'), ('LMM10','lmm10','A2W'), ('LMM10','lmm10','A3'), ('LMM10','lmm10','A3W'),
          ('LMM15','lmm15','A0'), ('LMM15','lmm15','A1'), ('LMM15','lmm15','A2'), ('LMM15','lmm15','A2W'), ('LMM15','lmm15','A3'), ('LMM15','lmm15','A3W'),
          ('LMM30','lmm30','A0'), ('LMM30','lmm30','A1'), ('LMM30','lmm30','A2'), ('LMM30','lmm30','A2W'), ('LMM30','lmm30','A3'), ('LMM30','lmm30','A3W'),
          ('ARIMA_M','arima_m','A0'), ('ARIMA_M','arima_m','A1'), ('ARIMA_M','arima_m','A2'), ('ARIMA_M','arima_m','A2W'), ('ARIMA_M','arima_m','A3'), ('ARIMA_M','arima_m','A3W'),
          ('SES_M','ses_m','A0'), ('SES_M','ses_m','A1'), ('SES_M','ses_m','A2'), ('SES_M','ses_m','A2W'), ('SES_M','ses_m','A3'), ('SES_M','ses_m','A3W'),
          ('HWES_M','hwes_m','A0'), ('HWES_M','hwes_m','A1'), ('HWES_M','hwes_m','A2'), ('HWES_M','hwes_m','A2W'), ('HWES_M','hwes_m','A3'), ('HWES_M','hwes_m','A3W')
      )
      SELECT
        sr.forecast_id,
        sr.date,
        round(h.value::numeric, 4)::numeric(18,4) AS value,
        %s               AS %I,
        %s               AS %I,
        (%L || '_' || v.column1 || '_' || v.column3)::text AS model_name,
        v.column2::text  AS base_model,
        round((CASE v.column2
          WHEN 'lmm1'    THEN %s
          WHEN 'lmm5'    THEN %s
          WHEN 'lmm10'   THEN %s
          WHEN 'lmm15'   THEN %s
          WHEN 'lmm30'   THEN %s
          WHEN 'arima_m' THEN %s
          WHEN 'ses_m'   THEN %s
          WHEN 'hwes_m'  THEN %s
        END)::numeric, 4)::numeric(18,4) AS base_fv,
        v.column3::text AS fmsr_series,
        round((CASE v.column3
          WHEN 'A0'  THEN 1::numeric
          WHEN 'A1'  THEN %s
          WHEN 'A2'  THEN %s
          WHEN 'A2W' THEN %s
          WHEN 'A3'  THEN %s
          WHEN 'A3W' THEN %s
        END)::numeric, 4)::numeric(18,4) AS fmsr_value,
        (
          (CASE v.column2
            WHEN 'lmm1'    THEN %s
            WHEN 'lmm5'    THEN %s
            WHEN 'lmm10'   THEN %s
            WHEN 'lmm15'   THEN %s
            WHEN 'lmm30'   THEN %s
            WHEN 'arima_m' THEN %s
            WHEN 'ses_m'   THEN %s
            WHEN 'hwes_m'  THEN %s
           END)::numeric
          *
          (CASE v.column3
            WHEN 'A0'  THEN 1::numeric
            WHEN 'A1'  THEN %s
            WHEN 'A2'  THEN %s
            WHEN 'A2W' THEN %s
            WHEN 'A3'  THEN %s
            WHEN 'A3W' THEN %s
           END)
        ) AS fv,
        NULL::numeric AS fv_error,
        NULL::numeric AS fv_mape,
        NULL::numeric AS fv_mean_mape,
        NULL::numeric AS fv_mean_mape_c,
        NULL::numeric AS fv_mae,
        NULL::numeric AS fv_mean_mae,
        NULL::numeric AS fv_mean_mae_c,
        NULL::numeric AS fv_rmse,
        NULL::numeric AS fv_mean_rmse,
        NULL::numeric AS fv_mean_rmse_c,
        NULL::text    AS mape_comparison,
        NULL::text    AS mean_mape_comparison,
        NULL::text    AS accuracy_comparison,
        NULL::int     AS best_mape_count,
        NULL::text    AS mae_comparison,
        NULL::text    AS mean_mae_comparison,
        NULL::text    AS mae_accuracy_comparison,
        NULL::int     AS best_mae_count,
        NULL::text    AS rmse_comparison,
        NULL::text    AS mean_rmse_comparison,
        NULL::text    AS rmse_accuracy_comparison,
        NULL::int     AS best_rmse_count,
        NULL::numeric AS fv_b125_u, NULL::numeric AS fv_b125_l, NULL::numeric AS fv_b150_u, NULL::numeric AS fv_b150_l, NULL::numeric AS fv_b175_u, NULL::numeric AS fv_b175_l, NULL::numeric AS fv_b200_u, NULL::numeric AS fv_b200_l, NULL::numeric AS fv_b225_u, NULL::numeric AS fv_b225_l, NULL::numeric AS fv_b250_u, NULL::numeric AS fv_b250_l, NULL::numeric AS fv_b275_u, NULL::numeric AS fv_b275_l, NULL::numeric AS fv_b300_u, NULL::numeric AS fv_b300_l, NULL::numeric AS fv_b325_u, NULL::numeric AS fv_b325_l, NULL::numeric AS fv_b350_u, NULL::numeric AS fv_b350_l,
        NULL::text AS b125_hit, NULL::text AS b150_hit, NULL::text AS b175_hit, NULL::text AS b200_hit, NULL::text AS b225_hit, NULL::text AS b250_hit, NULL::text AS b275_hit, NULL::text AS b300_hit, NULL::text AS b325_hit, NULL::text AS b350_hit,
        NULL::numeric AS b125_cov, NULL::numeric AS b125_cov_c, NULL::numeric AS b150_cov, NULL::numeric AS b150_cov_c, NULL::numeric AS b175_cov, NULL::numeric AS b175_cov_c, NULL::numeric AS b200_cov, NULL::numeric AS b200_cov_c, NULL::numeric AS b225_cov, NULL::numeric AS b225_cov_c, NULL::numeric AS b250_cov, NULL::numeric AS b250_cov_c, NULL::numeric AS b275_cov, NULL::numeric AS b275_cov_c, NULL::numeric AS b300_cov, NULL::numeric AS b300_cov_c, NULL::numeric AS b325_cov, NULL::numeric AS b325_cov_c, NULL::numeric AS b350_cov, NULL::numeric AS b350_cov_c,
        NULL::numeric AS ci85_low, NULL::numeric AS ci85_high,
        NULL::numeric AS ci90_low, NULL::numeric AS ci90_high,
        NULL::numeric AS ci95_low, NULL::numeric AS ci95_high,
        NULL::numeric AS fv_variance,
        NULL::numeric AS fv_variance_mean,
        NULL::numeric AS qm_msr,
        NULL::text    AS msr_dir,
        NULL::text    AS fmsr_dir,
        NULL::text    AS dir_hit,
        NULL::int     AS dir_hit_count
      FROM %s sr
      JOIN __ih_subset h
        ON h.forecast_id = sr.forecast_id
       AND h.date        = sr.date
      CROSS JOIN variants v
      WHERE sr.forecast_id = %L
      ORDER BY sr.date, v.column1, v.column3;
$f$, sr_base_col, 'series',
      sr_yqm_col,  base || '_yqm',
      base,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      sr_qual, latest_id);
EXECUTE sql;
DELETE FROM __tmp_forecast_build WHERE base_fv IS NULL;
UPDATE __tmp_forecast_build SET fv_error = ABS(value - fv), fv_mae = ABS(value - fv);
ANALYZE __tmp_forecast_build;


    EXECUTE format($i$ INSERT INTO %1$s (
        forecast_id, "date", value, %2$I, %3$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison, best_mape_count,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison, best_mae_count,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison, best_rmse_count,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l, fv_b225_u, fv_b225_l, fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l, fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b125_cov_c, b150_cov, b150_cov_c, b175_cov, b175_cov_c, b200_cov, b200_cov_c, b225_cov, b225_cov_c, b250_cov, b250_cov_c, b275_cov, b275_cov_c, b300_cov, b300_cov_c, b325_cov, b325_cov_c, b350_cov, b350_cov_c,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        fv_variance, fv_variance_mean,
        qm_msr, msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        created_at
      )
      SELECT
        forecast_id, "date", value, %4$I, %5$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, ABS(value - fv)::numeric AS fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison, best_mape_count,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison, best_mae_count,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison, best_rmse_count,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l, fv_b225_u, fv_b225_l, fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l, fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b125_cov_c, b150_cov, b150_cov_c, b175_cov, b175_cov_c, b200_cov, b200_cov_c, b225_cov, b225_cov_c, b250_cov, b250_cov_c, b275_cov, b275_cov_c, b300_cov, b300_cov_c, b325_cov, b325_cov_c, b350_cov, b350_cov_c,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        fv_variance, fv_variance_mean,
        qm_msr, msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        now()
      FROM __tmp_forecast_build
      ON CONFLICT (forecast_id, date, model_name, fmsr_series) DO NOTHING $i$, dest_qual, dest_series_col, dest_season_col, 'series', base || '_yqm');

    EXECUTE format($u$ UPDATE %1$s
       SET fv_mape = ABS(value - fv) / NULLIF(ABS(value),0)
     WHERE forecast_id = $2 AND value IS NOT NULL AND fv IS NOT NULL AND ABS(value) > 0 $u$, dest_qual) USING NULL, latest_id;

    RAISE NOTICE 'PASS 3 — season anatomy';
    EXECUTE 'DROP TABLE IF EXISTS __season_dim';
    EXECUTE format($u$ CREATE TEMP TABLE __season_dim AS
      SELECT
        %1$I AS series,
        model_name AS model_name,
        %2$I AS yqm,
        MIN(date)  AS season_start,
        AVG(fv_mape) AS season_mape,
        AVG(fv_mae)  AS season_mae,
        sqrt(AVG(POWER(ABS(value - fv),2))) AS season_rmse
      FROM %3$s
      WHERE forecast_id = $1 AND base_fv IS NOT NULL
      GROUP BY %1$I, model_name, %2$I $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __season_dim (series, model_name, yqm);
    ANALYZE __season_dim;

    EXECUTE format($u$ UPDATE %1$s t
       SET fv_rmse = sd.season_rmse
      FROM __season_dim sd
     WHERE t.%2$I = sd.series AND t.model_name = sd.model_name AND t.%3$I = sd.yqm $u$, dest_qual, dest_series_col, dest_season_col);

    RAISE NOTICE 'PASS 4 — rolling means';
    EXECUTE format($u$ WITH stats AS (
        SELECT
          s.series, s.model_name, s.yqm, s.season_start,
          s.season_mape, s.season_mae, s.season_rmse,
          SUM(s.season_mape) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mape_prev_sum,
          COUNT(s.season_mape) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mape_prev_cnt,
          SUM(s.season_mae)  OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mae_prev_sum,
          COUNT(s.season_mae) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mae_prev_cnt,
          SUM(s.season_rmse) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rmse_prev_sum,
          COUNT(s.season_rmse) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rmse_prev_cnt
        FROM __season_dim s
      )
      UPDATE %1$s t
         SET fv_mean_mape   = (st.mape_prev_sum / NULLIF(st.mape_prev_cnt,0)),
             fv_mean_mape_c = (st.mape_prev_cnt)::numeric,
             fv_mean_mae    = (st.mae_prev_sum / NULLIF(st.mae_prev_cnt,0)),
             fv_mean_mae_c  = (st.mae_prev_cnt)::numeric,
             fv_mean_rmse   = (st.rmse_prev_sum / NULLIF(st.rmse_prev_cnt,0)),
             fv_mean_rmse_c = (st.rmse_prev_cnt)::numeric
        FROM stats st
       WHERE t.%2$I = st.series AND t.model_name = st.model_name AND t.%3$I = st.yqm $u$, dest_qual, dest_series_col, dest_season_col);

    RAISE NOTICE 'PASS 5 — bands';
    EXECUTE format($u$ UPDATE %1$s
        SET
          fv_b125_u = fv + ((fv * fv_mean_mape) * 1.25), fv_b125_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.25)),
          fv_b150_u = fv + ((fv * fv_mean_mape) * 1.50), fv_b150_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.50)),
          fv_b175_u = fv + ((fv * fv_mean_mape) * 1.75), fv_b175_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.75)),
          fv_b200_u = fv + ((fv * fv_mean_mape) * 2.00), fv_b200_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.00)),
          fv_b225_u = fv + ((fv * fv_mean_mape) * 2.25), fv_b225_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.25)),
          fv_b250_u = fv + ((fv * fv_mean_mape) * 2.50), fv_b250_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.50)),
          fv_b275_u = fv + ((fv * fv_mean_mape) * 2.75), fv_b275_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.75)),
          fv_b300_u = fv + ((fv * fv_mean_mape) * 3.00), fv_b300_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.00)),
          fv_b325_u = fv + ((fv * fv_mean_mape) * 3.25), fv_b325_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.25)),
          fv_b350_u = fv + ((fv * fv_mean_mape) * 3.50), fv_b350_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.50))
        WHERE fv_mean_mape IS NOT NULL $u$, dest_qual);

    EXECUTE format($u$ UPDATE %1$s SET
          b125_hit = CASE WHEN value IS NULL OR fv_b125_l IS NULL OR fv_b125_u IS NULL THEN NULL WHEN value > fv_b125_l AND value < fv_b125_u THEN 'Y' ELSE 'N' END,
          b150_hit = CASE WHEN value IS NULL OR fv_b150_l IS NULL OR fv_b150_u IS NULL THEN NULL WHEN value > fv_b150_l AND value < fv_b150_u THEN 'Y' ELSE 'N' END,
          b175_hit = CASE WHEN value IS NULL OR fv_b175_l IS NULL OR fv_b175_u IS NULL THEN NULL WHEN value > fv_b175_l AND value < fv_b175_u THEN 'Y' ELSE 'N' END,
          b200_hit = CASE WHEN value IS NULL OR fv_b200_l IS NULL OR fv_b200_u IS NULL THEN NULL WHEN value > fv_b200_l AND value < fv_b200_u THEN 'Y' ELSE 'N' END,
          b225_hit = CASE WHEN value IS NULL OR fv_b225_l IS NULL OR fv_b225_u IS NULL THEN NULL WHEN value > fv_b225_l AND value < fv_b225_u THEN 'Y' ELSE 'N' END,
          b250_hit = CASE WHEN value IS NULL OR fv_b250_l IS NULL OR fv_b250_u IS NULL THEN NULL WHEN value > fv_b250_l AND value < fv_b250_u THEN 'Y' ELSE 'N' END,
          b275_hit = CASE WHEN value IS NULL OR fv_b275_l IS NULL OR fv_b275_u IS NULL THEN NULL WHEN value > fv_b275_l AND value < fv_b275_u THEN 'Y' ELSE 'N' END,
          b300_hit = CASE WHEN value IS NULL OR fv_b300_l IS NULL OR fv_b300_u IS NULL THEN NULL WHEN value > fv_b300_l AND value < fv_b300_u THEN 'Y' ELSE 'N' END,
          b325_hit = CASE WHEN value IS NULL OR fv_b325_l IS NULL OR fv_b325_u IS NULL THEN NULL WHEN value > fv_b325_l AND value < fv_b325_u THEN 'Y' ELSE 'N' END,
          b350_hit = CASE WHEN value IS NULL OR fv_b350_l IS NULL OR fv_b350_u IS NULL THEN NULL WHEN value > fv_b350_l AND value < fv_b350_u THEN 'Y' ELSE 'N' END
        WHERE forecast_id = $2 $u$, dest_qual) USING NULL, latest_id;

    EXECUTE 'DROP TABLE IF EXISTS __season_band';
    EXECUTE format($u$
      CREATE TEMP TABLE __season_band AS
      SELECT
        %1$I AS series, model_name, %2$I AS yqm,
        AVG(CASE WHEN b125_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b125_score, AVG(CASE WHEN b150_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b150_score, AVG(CASE WHEN b175_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b175_score, AVG(CASE WHEN b200_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b200_score, AVG(CASE WHEN b225_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b225_score, AVG(CASE WHEN b250_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b250_score, AVG(CASE WHEN b275_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b275_score, AVG(CASE WHEN b300_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b300_score, AVG(CASE WHEN b325_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b325_score, AVG(CASE WHEN b350_hit = 'Y' THEN 1.0 ELSE 0.0 END)::numeric AS b350_score
      FROM %3$s WHERE forecast_id = $1
      GROUP BY %1$I, model_name, %2$I $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __season_band (series, model_name, yqm);
    ANALYZE __season_band;

    EXECUTE 'DROP TABLE IF EXISTS __band_cov';
    EXECUTE $u$
      CREATE TEMP TABLE __band_cov AS
      WITH j AS (
        SELECT sb.*, sd.season_start
        FROM __season_band sb
        JOIN __season_dim sd
          ON sd.series = sb.series AND sd.model_name = sb.model_name AND sd.yqm = sb.yqm
      ), cov AS (
        SELECT
          j.series, j.model_name, j.yqm, j.season_start,
          AVG(b125_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b125_cov,
          AVG(b150_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b150_cov,
          AVG(b175_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b175_cov,
          AVG(b200_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b200_cov,
          AVG(b225_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b225_cov,
          AVG(b250_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b250_cov,
          AVG(b275_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b275_cov,
          AVG(b300_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b300_cov,
          AVG(b325_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b325_cov,
          AVG(b350_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b350_cov,
          COUNT(b125_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b125_cov_c,
          COUNT(b150_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b150_cov_c,
          COUNT(b175_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b175_cov_c,
          COUNT(b200_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b200_cov_c,
          COUNT(b225_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b225_cov_c,
          COUNT(b250_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b250_cov_c,
          COUNT(b275_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b275_cov_c,
          COUNT(b300_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b300_cov_c,
          COUNT(b325_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b325_cov_c,
          COUNT(b350_score) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b350_cov_c
        FROM j
      )
      SELECT * FROM cov
    $u$;
    CREATE INDEX ON __band_cov (series, model_name, yqm);
    ANALYZE __band_cov;

    EXECUTE format($u$ UPDATE %1$s t
         SET
           b125_cov = bc.b125_cov, b125_cov_c = bc.b125_cov_c,
           b150_cov = bc.b150_cov, b150_cov_c = bc.b150_cov_c,
           b175_cov = bc.b175_cov, b175_cov_c = bc.b175_cov_c,
           b200_cov = bc.b200_cov, b200_cov_c = bc.b200_cov_c,
           b225_cov = bc.b225_cov, b225_cov_c = bc.b225_cov_c,
           b250_cov = bc.b250_cov, b250_cov_c = bc.b250_cov_c,
           b275_cov = bc.b275_cov, b275_cov_c = bc.b275_cov_c,
           b300_cov = bc.b300_cov, b300_cov_c = bc.b300_cov_c,
           b325_cov = bc.b325_cov, b325_cov_c = bc.b325_cov_c,
           b350_cov = bc.b350_cov, b350_cov_c = bc.b350_cov_c
        FROM __band_cov bc
       WHERE t.%2$I = bc.series AND t.model_name = bc.model_name AND t.%3$I = bc.yqm $u$, dest_qual, dest_series_col, dest_season_col);

    RAISE NOTICE 'PASS 6 — CI selection';
    EXECUTE format($u$
      WITH cov AS (
        SELECT
          t.%1$I AS series, t.model_name, t.%2$I AS yqm,
          COALESCE(bc.b125_cov, NULL) AS b125_cov, COALESCE(bc.b150_cov, NULL) AS b150_cov, COALESCE(bc.b175_cov, NULL) AS b175_cov, COALESCE(bc.b200_cov, NULL) AS b200_cov, COALESCE(bc.b225_cov, NULL) AS b225_cov, COALESCE(bc.b250_cov, NULL) AS b250_cov, COALESCE(bc.b275_cov, NULL) AS b275_cov, COALESCE(bc.b300_cov, NULL) AS b300_cov, COALESCE(bc.b325_cov, NULL) AS b325_cov, COALESCE(bc.b350_cov, NULL) AS b350_cov
        FROM %3$s t
        LEFT JOIN __band_cov bc
          ON bc.series = t.%1$I AND bc.model_name = t.model_name AND bc.yqm = t.%2$I
        WHERE t.forecast_id = $1
        GROUP BY t.%1$I, t.model_name, t.%2$I, COALESCE(bc.b125_cov, NULL), COALESCE(bc.b150_cov, NULL), COALESCE(bc.b175_cov, NULL), COALESCE(bc.b200_cov, NULL), COALESCE(bc.b225_cov, NULL), COALESCE(bc.b250_cov, NULL), COALESCE(bc.b275_cov, NULL), COALESCE(bc.b300_cov, NULL), COALESCE(bc.b325_cov, NULL), COALESCE(bc.b350_cov, NULL)
),
      chosen AS (
        SELECT c.*,
          (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) AS ci85_mult,
          (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) AS ci90_mult,
          (CASE WHEN c.b125_cov >= 0.95 AND 1.25 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 1.25 WHEN c.b150_cov >= 0.95 AND 1.50 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 1.50 WHEN c.b175_cov >= 0.95 AND 1.75 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 1.75 WHEN c.b200_cov >= 0.95 AND 2.00 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 2.00 WHEN c.b225_cov >= 0.95 AND 2.25 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 2.25 WHEN c.b250_cov >= 0.95 AND 2.50 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 2.50 WHEN c.b275_cov >= 0.95 AND 2.75 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 2.75 WHEN c.b300_cov >= 0.95 AND 3.00 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 3.00 WHEN c.b325_cov >= 0.95 AND 3.25 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 3.25 WHEN c.b350_cov >= 0.95 AND 3.50 > (CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.90 AND 1.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.25 WHEN c.b150_cov >= 0.90 AND 1.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.50 WHEN c.b175_cov >= 0.90 AND 1.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 1.75 WHEN c.b200_cov >= 0.90 AND 2.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.00 WHEN c.b225_cov >= 0.90 AND 2.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.25 WHEN c.b250_cov >= 0.90 AND 2.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.50 WHEN c.b275_cov >= 0.90 AND 2.75 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 2.75 WHEN c.b300_cov >= 0.90 AND 3.00 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.00 WHEN c.b325_cov >= 0.90 AND 3.25 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.25 WHEN c.b350_cov >= 0.90 AND 3.50 > (CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END) THEN 3.50 ELSE LEAST((CASE WHEN c.b125_cov >= 0.85 THEN 1.25 WHEN c.b150_cov >= 0.85 THEN 1.50 WHEN c.b175_cov >= 0.85 THEN 1.75 WHEN c.b200_cov >= 0.85 THEN 2.00 WHEN c.b225_cov >= 0.85 THEN 2.25 WHEN c.b250_cov >= 0.85 THEN 2.50 WHEN c.b275_cov >= 0.85 THEN 2.75 WHEN c.b300_cov >= 0.85 THEN 3.00 WHEN c.b325_cov >= 0.85 THEN 3.25 WHEN c.b350_cov >= 0.85 THEN 3.50 ELSE 3.00 END)+0.25, 3.25) END)+0.25, 3.50) END) AS ci95_mult
        FROM cov c
      )
      UPDATE %4$s t
         SET
           ci85_low = GREATEST(0, fv - (fv * fv_mean_mape * ci85_mult)),
           ci85_high = fv + (fv * fv_mean_mape * ci85_mult),
           ci90_low = GREATEST(0, fv - (fv * fv_mean_mape * ci90_mult)),
           ci90_high = fv + (fv * fv_mean_mape * ci90_mult),
           ci95_low = GREATEST(0, fv - (fv * fv_mean_mape * ci95_mult)),
           ci95_high = fv + (fv * fv_mean_mape * ci95_mult)
        FROM chosen ch
       WHERE t.%1$I = ch.series AND t.model_name = ch.model_name AND t.%2$I = ch.yqm $u$,
       dest_series_col, dest_season_col, dest_qual, dest_qual) USING latest_id;

    RAISE NOTICE 'PASS 7 — A0/Ax comps + counts';
        -- PASS7 RUNFIX: speed up updates by indexing destination table for this run
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ' || replace(dest_qual,'.','_') || '__fi ON ' || dest_qual || ' (forecast_id)';
    EXCEPTION WHEN others THEN NULL; END;
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ' || replace(dest_qual,'.','_') || '__key ON ' || dest_qual || ' (' || dest_series_col || ', model_name, ' || dest_season_col || ')';
    EXCEPTION WHEN others THEN NULL; END;
    EXECUTE 'ANALYZE ' || dest_qual || ';';
EXECUTE 'DROP TABLE IF EXISTS __a0_map';
    EXECUTE format($u$ CREATE TEMP TABLE __a0_map AS
      SELECT base_model, %1$I AS yqm,
             MAX(fv_mape)      AS mape0,
             MAX(fv_mean_mape) AS mean_mape0,
             MAX(fv_mae)       AS mae0,
             MAX(fv_mean_mae)  AS mean_mae0,
             MAX(fv_rmse)      AS rmse0,
             MAX(fv_mean_rmse) AS mean_rmse0
      FROM %2$s WHERE forecast_id = $1 AND fmsr_series = 'A0'
      GROUP BY base_model, %1$I $u$, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __a0_map (base_model, yqm);
    ANALYZE __a0_map;

    EXECUTE format($u$ UPDATE %2$s t
         SET
           mape_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mape IS NULL OR a0.mape0 IS NULL THEN NULL
                  WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H' END,
           mean_mape_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
                  WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H' END,
           accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mape IS NULL OR a0.mape0 IS NULL OR t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END,
           mae_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mae IS NULL OR a0.mae0 IS NULL THEN NULL
                  WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H' END,
           mean_mae_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
                  WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H' END,
           mae_accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mae IS NULL OR a0.mae0 IS NULL OR t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END,
           rmse_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_rmse IS NULL OR a0.rmse0 IS NULL THEN NULL
                  WHEN t.fv_rmse < a0.rmse0 THEN 'L' ELSE 'H' END,
           mean_rmse_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_mean_rmse IS NULL OR a0.mean_rmse0 IS NULL THEN NULL
                  WHEN t.fv_mean_rmse < a0.mean_rmse0 THEN 'L' ELSE 'H' END,
           rmse_accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' OR t.fv_rmse IS NULL OR a0.rmse0 IS NULL OR t.fv_mean_rmse IS NULL OR a0.mean_rmse0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_rmse < a0.rmse0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_rmse < a0.mean_rmse0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END
      FROM __a0_map a0
     WHERE t.base_model = a0.base_model AND t.%1$I = a0.yqm $u$, dest_season_col, dest_qual) USING latest_id;

    EXECUTE format($u$
      WITH flags AS (
        SELECT %1$I AS series, model_name, %2$I AS yqm,
               MAX(CASE WHEN accuracy_comparison='Y' THEN 1 ELSE 0 END) AS acc_mape,
               MAX(CASE WHEN mae_accuracy_comparison='Y' THEN 1 ELSE 0 END) AS acc_mae,
               MAX(CASE WHEN rmse_accuracy_comparison='Y' THEN 1 ELSE 0 END) AS acc_rmse
        FROM %3$s WHERE forecast_id=$1
        GROUP BY %1$I, model_name, %2$I
      ),
      j AS (
        SELECT f.*, sd.season_start
        FROM flags f JOIN __season_dim sd
          ON sd.series=f.series AND sd.model_name=f.model_name AND sd.yqm=f.yqm
      ),
      stats AS (
        SELECT
          j.series, j.model_name, j.yqm,
          SUM(j.acc_mape) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mape_count,
          SUM(j.acc_mae)  OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mae_count,
          SUM(j.acc_rmse) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_rmse_count
        FROM j
      )
      UPDATE %3$s t
         SET best_mape_count = st.best_mape_count::int,
             best_mae_count  = st.best_mae_count::int,
             best_rmse_count = st.best_rmse_count::int
        FROM stats st
       WHERE t.%1$I=st.series AND t.model_name=st.model_name AND t.%2$I=st.yqm $u$,
       dest_series_col, dest_season_col, dest_qual) USING latest_id;

    RAISE NOTICE 'PASS 7b — variability (direction)';
    EXECUTE 'DROP TABLE IF EXISTS __season_var';
    EXECUTE format($u$ CREATE TEMP TABLE __season_var AS
      SELECT
        %1$I AS series, model_name, %2$I AS yqm,
        MIN(date) AS season_start,
        MAX(CASE WHEN fmsr_series=''A0'' THEN fmsr_value END) AS qm_msr,
        MAX(fmsr_value) AS fmsr_any
      FROM %3$s WHERE forecast_id = $1
      GROUP BY %1$I, model_name, %2$I $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __season_var (series, model_name, yqm);
    ANALYZE __season_var;

    EXECUTE $v$
      WITH j AS (
        SELECT
          sv.model_name,
          sv.yqm,
          sv.season_start,
          sv.qm_msr,
          COALESCE(sv.qm_msr, sv.fmsr_any) AS fmsr_season
        FROM __season_var sv
      ),
      d AS (
        SELECT j.*,
               LAG(j.qm_msr) OVER (PARTITION BY j.model_name ORDER BY j.season_start) AS prev_msr,
               LAG(j.fmsr_season) OVER (PARTITION BY j.model_name ORDER BY j.season_start) AS prev_fmsr
        FROM j
      ),
      flags AS (
        SELECT d.*,
               CASE WHEN prev_msr IS NULL OR qm_msr IS NULL THEN NULL
                    WHEN qm_msr > prev_msr THEN 'U'
                    WHEN qm_msr < prev_msr THEN 'D'
                    ELSE NULL END AS msr_dir,
               CASE WHEN prev_fmsr IS NULL OR fmsr_season IS NULL THEN NULL
                    WHEN fmsr_season > prev_fmsr THEN 'U'
                    WHEN fmsr_season < prev_fmsr THEN 'D'
                    ELSE NULL END AS fmsr_dir
        FROM d
      ),
      hits AS (
        SELECT f.*,
               CASE WHEN f.msr_dir IS NOT NULL AND f.fmsr_dir IS NOT NULL AND f.msr_dir = f.fmsr_dir THEN 'Y'
                    WHEN f.msr_dir IS NOT NULL AND f.fmsr_dir IS NOT NULL THEN 'N'
                    ELSE NULL END AS dir_hit
        FROM flags f
      ),
      cum AS (
        SELECT h.*,
               SUM(CASE WHEN h.dir_hit='Y' THEN 1 ELSE 0 END) OVER (
                 PARTITION BY h.model_name ORDER BY h.season_start
                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ) AS dir_hit_count
        FROM hits h
      )
      UPDATE __work t
         SET qm_msr = c.qm_msr,
             msr_dir = c.msr_dir,
             fmsr_dir = c.fmsr_dir,
             dir_hit = c.dir_hit,
             dir_hit_count = COALESCE(c.dir_hit_count,0)
        FROM cum c
       WHERE t.model_name = c.model_name AND t.season = c.yqm
    $v$;

    RAISE NOTICE 'PASS 8 — clamp to 4dp';
    EXECUTE format($c$ UPDATE %s SET
        value            = round(value::numeric, 4),
        base_fv          = round(base_fv::numeric, 4),
        fmsr_value       = round(fmsr_value::numeric, 4),
        fv               = round(fv::numeric, 4),
        fv_error         = CASE WHEN fv_error IS NULL THEN NULL ELSE round(fv_error::numeric, 4) END,
        fv_mape          = CASE WHEN fv_mape IS NULL THEN NULL ELSE round(fv_mape::numeric, 4) END,
        fv_mean_mape     = CASE WHEN fv_mean_mape IS NULL THEN NULL ELSE round(fv_mean_mape::numeric, 4) END,
        fv_mae           = CASE WHEN fv_mae IS NULL THEN NULL ELSE round(fv_mae::numeric, 4) END,
        fv_mean_mae      = CASE WHEN fv_mean_mae IS NULL THEN NULL ELSE round(fv_mean_mae::numeric, 4) END,
        fv_rmse          = CASE WHEN fv_rmse IS NULL THEN NULL ELSE round(fv_rmse::numeric, 4) END,
        fv_mean_rmse     = CASE WHEN fv_mean_rmse IS NULL THEN NULL ELSE round(fv_mean_rmse::numeric, 4) END,
        fv_variance      = CASE WHEN fv_variance IS NULL THEN NULL ELSE round(fv_variance::numeric, 4) END,
        fv_variance_mean = CASE WHEN fv_variance_mean IS NULL THEN NULL ELSE round(fv_variance_mean::numeric, 4) END
      $c$, dest_qual);

    PERFORM set_config('synchronous_commit','off',true);
    EXECUTE format('DELETE FROM %s WHERE forecast_id = $1', dest_real_qual) USING latest_id;
    EXECUTE 'INSERT INTO '||dest_real_qual||' SELECT * FROM __work WHERE forecast_id = $1' USING latest_id;
    RAISE NOTICE 'COMPLETE series: % (elapsed %.3f s)',
          dest_rel, EXTRACT(epoch FROM clock_timestamp() - t_series_start);

    RAISE NOTICE 'forecast=% base=% done_at=% dur=%',
      v_forecast_name, base, clock_timestamp(), clock_timestamp()-t_series_start;
    RAISE NOTICE 'series done  forecast=% base=% at=% dur=%',
      v_forecast_name, base, clock_timestamp(), clock_timestamp()-t_series_start;
END LOOP;

  BEGIN
    PERFORM 1 FROM information_schema.columns
     WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msqm_complete';
    IF FOUND THEN
      PERFORM 1 FROM information_schema.columns
       WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msqm_complete'
         AND data_type IN ('timestamp without time zone','timestamp with time zone','date','time without time zone','time with time zone');
      IF FOUND THEN
        EXECUTE format('UPDATE engine.forecast_registry SET msqm_complete = now() WHERE forecast_id = $1') USING latest_id;
      ELSE
        PERFORM 1 FROM information_schema.columns
         WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msqm_complete' AND data_type IN ('boolean');
        IF FOUND THEN
          EXECUTE format('UPDATE engine.forecast_registry SET msqm_complete = true WHERE forecast_id = $1') USING latest_id;
        ELSE
          EXECUTE format('UPDATE engine.forecast_registry SET msqm_complete = %s WHERE forecast_id = $1', quote_literal('complete')) USING latest_id;
        END IF;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Registry update for msqm_complete failed: %', SQLERRM;
  END;

  RAISE NOTICE 'ALL DONE';
END
$$;

GRANT EXECUTE ON FUNCTION engine.ms_forecast(text) TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.ms_forecast__core() TO matrix_reader, tsf_engine_app;
