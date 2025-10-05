-- V14_10_FORECAST_MSQM.sql
-- 2025-10-05: V14 — MSQM variant: Cannon logic (bands+CI 85/90/95, MAPE/MAE/RMSE, lagged counts, coverage, variance).
-- Keep V13 wrapper/function names and flow; apply only Cannon column/logic updates in core.
-- No references to engine.wd_md_instance_forecast_*, no sr_* injection; dynamic series/season column resolution.
-- DO NOT CHANGE WRAPPER NAMES OR SIGNATURES.

SET client_min_messages = NOTICE;

CREATE OR REPLACE FUNCTION engine.msqm_forecast(forecast_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  PERFORM set_config('engine.forecast_name', forecast_name, true);
  PERFORM engine.msqm_forecast__core();
  PERFORM set_config('engine.forecast_name', '', true);
END;
$$;

CREATE OR REPLACE FUNCTION engine.msqm_forecast__core()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_forecast_name         text;
  forecast_target_id      uuid;
  t_run_start             timestamptz := clock_timestamp();
  t_series_start          timestamptz;
  r                       record;
  base                    text;
  sr_rel                  text;
  sr_qual                 text;
  latest_id               uuid;
  start_from              date;
  dest_rel                text;
  dest_qual               text;
  dest_real_qual          text;
  dest_series_col         text;
  dest_season_col         text;
  sr_base_col             text;
  sr_yqm_col              text;
  sr_fmsr_a1_col          text;
  sr_fmsr_a2_col          text;
  sr_fmsr_a2w_col         text;
  sr_fmsr_a3_col          text;
  sr_fmsr_a3w_col         text;
  -- classical helpers (hydration from __ih_subset alias h)
  h_lmm1                  text := 'h.'||quote_ident('lmm1');
  h_lmm5                  text := 'h.'||quote_ident('lmm5');
  h_lmm10                 text := 'h.'||quote_ident('lmm10');
  h_lmm15                 text := 'h.'||quote_ident('lmm15');
  h_lmm30                 text := 'h.'||quote_ident('lmm30');
  h_arima_m               text := 'h.'||quote_ident('arima_m');
  h_ses_m                 text := 'h.'||quote_ident('ses_m');
  h_hwes_m                text := 'h.'||quote_ident('hwes_m');
  sql                     text;
  rcnt                    bigint;
  tname                   text;
BEGIN
  v_forecast_name := current_setting('engine.forecast_name', true);
  IF v_forecast_name IS NULL THEN
    RAISE EXCEPTION 'engine.msqm_forecast() requires forecast_name';
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
  CREATE INDEX ON __ih_subset (date);
  ANALYZE __ih_subset;

  -- Drop any leftover forecast_s scratch tables
  FOR tname IN
    SELECT format('%I.%I', 'engine', c.relname)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'engine'
      AND c.relkind = 'r'
      AND c.relname LIKE '%\_forecast\_s' ESCAPE '\'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', tname);
  END LOOP;

  -- Iterate each *_instance_sr_s series
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
    sr_qual := format('%I.%I', 'engine', sr_rel);
    RAISE NOTICE 'BEGIN series: %', base;

    -- Start-from: 2 years after min date
    EXECUTE $q$
      SELECT (min(date) + interval '2 years')::date
      FROM __ih_subset
      WHERE forecast_id = $1
    $q$ USING latest_id INTO start_from;

    IF start_from IS NULL THEN
      RAISE NOTICE 'SKIP series % — no historical', base;
      CONTINUE;
    END IF;

    dest_rel  := base || '_instance_forecast_msqm';
    dest_qual := format('%I.%I', 'engine', dest_rel);

    -- Create destination table if missing (Cannon schema)
    IF to_regclass(dest_qual) IS NULL THEN
      EXECUTE format($ct$
        CREATE TABLE %1$s (
          forecast_id uuid NOT NULL,
          date date NOT NULL,
          value numeric(18,4),
          series text,
          season text,
          model_name text,
          base_model text,
          base_fv numeric(18,4),
          fmsr_series text,
          fmsr_value numeric(18,4),
          fv numeric(18,4),
          fv_error numeric(18,4),
          -- MAPE/MAE/RMSE
          fv_mape numeric(18,4),
          fv_mean_mape numeric(18,4),
          fv_mean_mape_c numeric(18,4),
          fv_mae numeric(18,4),
          fv_mean_mae numeric(18,4),
          fv_mean_mae_c numeric(18,4),
          fv_rmse numeric(18,4),
          fv_mean_rmse numeric(18,4),
          fv_mean_rmse_c numeric(18,4),
          -- A0/Ax comparisons
          mape_comparison text,
          mean_mape_comparison text,
          accuracy_comparison text,
          mae_comparison text,
          mean_mae_comparison text,
          mae_accuracy_comparison text,
          rmse_comparison text,
          mean_rmse_comparison text,
          rmse_accuracy_comparison text,
          -- Lagged counts
          best_mape_count integer,
          best_mae_count integer,
          best_rmse_count integer,
          -- 10-band bounds + hits
          fv_b125_u numeric(18,4), fv_b125_l numeric(18,4),
          fv_b150_u numeric(18,4), fv_b150_l numeric(18,4),
          fv_b175_u numeric(18,4), fv_b175_l numeric(18,4),
          fv_b200_u numeric(18,4), fv_b200_l numeric(18,4),
          fv_b225_u numeric(18,4), fv_b225_l numeric(18,4),
          fv_b250_u numeric(18,4), fv_b250_l numeric(18,4),
          fv_b275_u numeric(18,4), fv_b275_l numeric(18,4),
          fv_b300_u numeric(18,4), fv_b300_l numeric(18,4),
          fv_b325_u numeric(18,4), fv_b325_l numeric(18,4),
          fv_b350_u numeric(18,4), fv_b350_l numeric(18,4),
          b125_hit text, b150_hit text, b175_hit text, b200_hit text, b225_hit text,
          b250_hit text, b275_hit text, b300_hit text, b325_hit text, b350_hit text,
          -- Prior-season coverage
          b125_cov numeric(18,4), b150_cov numeric(18,4), b175_cov numeric(18,4), b200_cov numeric(18,4), b225_cov numeric(18,4),
          b250_cov numeric(18,4), b275_cov numeric(18,4), b300_cov numeric(18,4), b325_cov numeric(18,4), b350_cov numeric(18,4),
          -- Selected CI bounds
          ci85_low numeric(18,4), ci85_high numeric(18,4),
          ci90_low numeric(18,4), ci90_high numeric(18,4),
          ci95_low numeric(18,4), ci95_high numeric(18,4),
          -- Variability placeholders (left null if not available in msqm)
          msr_dir text,
          fmsr_dir text,
          dir_hit text,
          dir_hit_count integer,
          -- Variance
          fv_variance numeric(18,4),
          fv_variance_mean numeric(18,4),
          created_at timestamptz DEFAULT now(),
          PRIMARY KEY (forecast_id, date, model_name, fmsr_series)
        )
      $ct$, dest_qual);
    END IF;

    -- Work table mirror
    EXECUTE 'DROP TABLE IF EXISTS __work';
    EXECUTE 'CREATE TEMP TABLE __work (LIKE ' || dest_qual || ' INCLUDING ALL)';
    EXECUTE 'CREATE INDEX ON __work (forecast_id, date)';
    ANALYZE __work;
    dest_real_qual := dest_qual;
    dest_qual := '__work';

    -- Determine destination column names (series/season) — follow V13: series→s→base ; season→s_yqm→base||'_yqm'
    dest_series_col := 'series';
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_series_col;
    IF NOT FOUND THEN
      dest_series_col := 's';
      PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_series_col;
      IF NOT FOUND THEN dest_series_col := base; END IF;
    END IF;

    dest_season_col := 'season';
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_season_col;
    IF NOT FOUND THEN
      dest_season_col := 's_yqm';
      PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_season_col;
      IF NOT FOUND THEN dest_season_col := base || '_yqm'; END IF;
    END IF;

    -- source SR columns
    sr_base_col       := 'sr.' || quote_ident(base);
    sr_yqm_col        := 'sr.' || quote_ident(base || '_yqm');
    sr_fmsr_a1_col    := 'sr.' || quote_ident(base || '_fmsr_a1');
    sr_fmsr_a2_col    := 'sr.' || quote_ident(base || '_fmsr_a2');
    sr_fmsr_a2w_col   := 'sr.' || quote_ident(base || '_fmsr_a2w');
    sr_fmsr_a3_col    := 'sr.' || quote_ident(base || '_fmsr_a3');
    sr_fmsr_a3w_col   := 'sr.' || quote_ident(base || '_fmsr_a3w');

    -- PASS 1 — Hydration
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
        round(h.value::numeric, 4)::numeric(18,4) as value,
        %s               as %I,
        %s               as %I,
        (%L || '_' || v.column1 || '_' || v.column3)::text as model_name,
        v.column2::text  as base_model,
        round((case v.column2
          when 'lmm1'    then %s
          when 'lmm5'    then %s
          when 'lmm10'   then %s
          when 'lmm15'   then %s
          when 'lmm30'   then %s
          when 'arima_m' then %s
          when 'ses_m'   then %s
          when 'hwes_m'  then %s
        end)::numeric, 4)::numeric(18,4) as base_fv,
        v.column3::text as fmsr_series,
        round((case v.column3
          when 'A0'  then 1::numeric
          when 'A1'  then %s
          when 'A2'  then %s
          when 'A2W' then %s
          when 'A3'  then %s
          when 'A3W' then %s
        end)::numeric, 4)::numeric(18,4) as fmsr_value,
        (
          (case v.column2
            when 'lmm1'    then %s
            when 'lmm5'    then %s
            when 'lmm10'   then %s
            when 'lmm15'   then %s
            when 'lmm30'   then %s
            when 'arima_m' then %s
            when 'ses_m'   then %s
            when 'hwes_m'  then %s
           end)::numeric
          *
          (case v.column3
            when 'A0'  then 1::numeric
            when 'A1'  then %s
            when 'A2'  then %s
            when 'A2W' then %s
            when 'A3'  then %s
            when 'A3W' then %s
           end)
        ) as fv,
        NULL::numeric(18,4) as fv_error,
        NULL::numeric(18,4) as fv_mape,
        NULL::numeric(18,4) as fv_mean_mape,
        NULL::numeric(18,4) as fv_mean_mape_c,
        NULL::numeric(18,4) as fv_mae,
        NULL::numeric(18,4) as fv_mean_mae,
        NULL::numeric(18,4) as fv_mean_mae_c,
        NULL::numeric(18,4) as fv_rmse,
        NULL::numeric(18,4) as fv_mean_rmse,
        NULL::numeric(18,4) as fv_mean_rmse_c,
        NULL::text    as mape_comparison,
        NULL::text    as mean_mape_comparison,
        NULL::text    as accuracy_comparison,
        NULL::text    as mae_comparison,
        NULL::text    as mean_mae_comparison,
        NULL::text    as mae_accuracy_comparison,
        NULL::text    as rmse_comparison,
        NULL::text    as mean_rmse_comparison,
        NULL::text    as rmse_accuracy_comparison,
        NULL::int     as best_mape_count,
        NULL::int     as best_mae_count,
        NULL::int     as best_rmse_count,
        -- band bounds
        NULL::numeric(18,4) as fv_b125_u, NULL::numeric(18,4) as fv_b125_l,
        NULL::numeric(18,4) as fv_b150_u, NULL::numeric(18,4) as fv_b150_l,
        NULL::numeric(18,4) as fv_b175_u, NULL::numeric(18,4) as fv_b175_l,
        NULL::numeric(18,4) as fv_b200_u, NULL::numeric(18,4) as fv_b200_l,
        NULL::numeric(18,4) as fv_b225_u, NULL::numeric(18,4) as fv_b225_l,
        NULL::numeric(18,4) as fv_b250_u, NULL::numeric(18,4) as fv_b250_l,
        NULL::numeric(18,4) as fv_b275_u, NULL::numeric(18,4) as fv_b275_l,
        NULL::numeric(18,4) as fv_b300_u, NULL::numeric(18,4) as fv_b300_l,
        NULL::numeric(18,4) as fv_b325_u, NULL::numeric(18,4) as fv_b325_l,
        NULL::numeric(18,4) as fv_b350_u, NULL::numeric(18,4) as fv_b350_l,
        NULL::text as b125_hit, NULL::text as b150_hit, NULL::text as b175_hit, NULL::text as b200_hit, NULL::text as b225_hit,
        NULL::text as b250_hit, NULL::text as b275_hit, NULL::text as b300_hit, NULL::text as b325_hit, NULL::text as b350_hit,
        NULL::numeric(18,4) as b125_cov, NULL::numeric(18,4) as b150_cov, NULL::numeric(18,4) as b175_cov, NULL::numeric(18,4) as b200_cov, NULL::numeric(18,4) as b225_cov,
        NULL::numeric(18,4) as b250_cov, NULL::numeric(18,4) as b275_cov, NULL::numeric(18,4) as b300_cov, NULL::numeric(18,4) as b325_cov, NULL::numeric(18,4) as b350_cov,
        NULL::numeric(18,4) as ci85_low, NULL::numeric(18,4) as ci85_high,
        NULL::numeric(18,4) as ci90_low, NULL::numeric(18,4) as ci90_high,
        NULL::numeric(18,4) as ci95_low, NULL::numeric(18,4) as ci95_high,
        NULL::text as msr_dir,
        NULL::text as fmsr_dir,
        NULL::text as dir_hit,
        NULL::int  as dir_hit_count,
        NULL::numeric(18,4) as fv_variance,
        NULL::numeric(18,4) as fv_variance_mean
      FROM %s sr
      JOIN __ih_subset h
        ON h.forecast_id = sr.forecast_id
       AND h.date        = sr.date
      CROSS JOIN variants v
      WHERE sr.forecast_id = %L
      ORDER BY sr.date, v.column1, v.column3;
      DELETE FROM __tmp_forecast_build WHERE base_fv IS NULL;
      UPDATE __tmp_forecast_build
         SET fv_error = ABS(value - fv),
             fv_mae   = ABS(value - fv);
      ANALYZE __tmp_forecast_build;
    $f$,
      sr_base_col, dest_series_col,
      sr_yqm_col,  dest_season_col,
      base,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      sr_qual, latest_id
    );
    EXECUTE sql;

    -- INSERT into work table
    EXECUTE format($i$
      INSERT INTO %1$s (
        forecast_id, "date", value, %2$I, %3$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison,
        best_mape_count, best_mae_count, best_rmse_count,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l, fv_b225_u, fv_b225_l,
        fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l, fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        fv_variance, fv_variance_mean, created_at
      )
      SELECT
        forecast_id, "date", value, %2$I, %3$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison,
        best_mape_count, best_mae_count, best_rmse_count,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l, fv_b225_u, fv_b225_l,
        fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l, fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        fv_variance, fv_variance_mean, now()
      FROM __tmp_forecast_build
      ON CONFLICT (forecast_id, date, model_name, fmsr_series) DO NOTHING
    $i$, dest_qual, dest_series_col, dest_season_col);

    -- Season metrics
    EXECUTE 'DROP TABLE IF EXISTS __season_dim';
    EXECUTE format($u$
      CREATE TEMP TABLE __season_dim AS
      SELECT
        %1$I AS series,
        model_name,
        %2$I AS yqm,
        MIN(date)  AS season_start,
        AVG(ABS(value - fv) / NULLIF(ABS(value),0))::numeric AS season_mape,
        AVG(ABS(value - fv))::numeric AS season_mae,
        sqrt(AVG(POWER(ABS(value - fv),2)))::numeric AS season_rmse
      FROM %3$s WHERE forecast_id = $1 AND base_fv IS NOT NULL
      GROUP BY %1$I, model_name, %2$I
    $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;

    EXECUTE format($u$
      UPDATE %1$s t
         SET fv_mape = s.season_mape,
             fv_rmse = s.season_rmse
        FROM __season_dim s
       WHERE t.%2$I = s.series AND t.model_name = s.model_name AND t.%3$I = s.yqm
    $u$, dest_qual, dest_series_col, dest_season_col) USING latest_id;

    -- Rolling prior-season means
    EXECUTE format($u$ WITH stats AS (
        SELECT
          s.series,
          s.model_name,
          s.yqm,
          SUM(s.season_mape) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mape_sum,
          COUNT(s.season_mape) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mape_cnt,
          SUM(s.season_mae) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)  AS mae_sum,
          COUNT(s.season_mae) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)  AS mae_cnt,
          SUM(s.season_rmse) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rmse_sum,
          COUNT(s.season_rmse) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rmse_cnt
        FROM __season_dim s
      )
      UPDATE %1$s t
         SET fv_mean_mape   = (st.mape_sum / NULLIF(st.mape_cnt,0)),
             fv_mean_mape_c = st.mape_cnt::numeric,
             fv_mean_mae    = (st.mae_sum  / NULLIF(st.mae_cnt,0)),
             fv_mean_mae_c  = st.mae_cnt::numeric,
             fv_mean_rmse   = (st.rmse_sum / NULLIF(st.rmse_cnt,0)),
             fv_mean_rmse_c = st.rmse_cnt::numeric
        FROM stats st
       WHERE t.%2$I = st.series AND t.model_name = st.model_name AND t.%3$I = st.yqm
    $u$, dest_qual, dest_series_col, dest_season_col) USING latest_id;

    -- Bands + hits
    EXECUTE format($u$
      UPDATE %1$s
         SET
           fv_b125_u = fv + ((fv * fv_mean_mape) * 1.25),
           fv_b125_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.25)),
           fv_b150_u = fv + ((fv * fv_mean_mape) * 1.50),
           fv_b150_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.50)),
           fv_b175_u = fv + ((fv * fv_mean_mape) * 1.75),
           fv_b175_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.75)),
           fv_b200_u = fv + ((fv * fv_mean_mape) * 2.00),
           fv_b200_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.00)),
           fv_b225_u = fv + ((fv * fv_mean_mape) * 2.25),
           fv_b225_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.25)),
           fv_b250_u = fv + ((fv * fv_mean_mape) * 2.50),
           fv_b250_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.50)),
           fv_b275_u = fv + ((fv * fv_mean_mape) * 2.75),
           fv_b275_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 2.75)),
           fv_b300_u = fv + ((fv * fv_mean_mape) * 3.00),
           fv_b300_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.00)),
           fv_b325_u = fv + ((fv * fv_mean_mape) * 3.25),
           fv_b325_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.25)),
           fv_b350_u = fv + ((fv * fv_mean_mape) * 3.50),
           fv_b350_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 3.50))
       WHERE fv_mean_mape IS NOT NULL
    $u$, dest_qual) USING latest_id;

    EXECUTE format($u$
      UPDATE %1$s
         SET
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
    $u$, dest_qual) USING latest_id;

    -- Coverage per season
    EXECUTE 'DROP TABLE IF EXISTS __band_scores';
    EXECUTE format($u$
      CREATE TEMP TABLE __band_scores AS
      SELECT
        %1$I AS series, model_name, %2$I AS yqm,
        AVG(CASE WHEN b125_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc125,
        AVG(CASE WHEN b150_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc150,
        AVG(CASE WHEN b175_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc175,
        AVG(CASE WHEN b200_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc200,
        AVG(CASE WHEN b225_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc225,
        AVG(CASE WHEN b250_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc250,
        AVG(CASE WHEN b275_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc275,
        AVG(CASE WHEN b300_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc300,
        AVG(CASE WHEN b325_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc325,
        AVG(CASE WHEN b350_hit = 'Y' THEN 1.0 ELSE 0.0 END) AS sc350
      FROM %3$s WHERE forecast_id = $1
      GROUP BY %1$I, model_name, %2$I
    $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;

    EXECUTE format($u$ WITH s AS (
        SELECT sd.series, sd.model_name, sd.yqm, sd.season_start,
               bs.sc125, bs.sc150, bs.sc175, bs.sc200, bs.sc225, bs.sc250, bs.sc275, bs.sc300, bs.sc325, bs.sc350
        FROM __season_dim sd
        LEFT JOIN __band_scores bs
          ON bs.series = sd.series AND bs.model_name = sd.model_name AND bs.yqm = sd.yqm
      ),
      cov AS (
        SELECT
          s.series, s.model_name, s.yqm,
          AVG(s.sc125) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c125,
          AVG(s.sc150) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c150,
          AVG(s.sc175) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c175,
          AVG(s.sc200) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c200,
          AVG(s.sc225) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c225,
          AVG(s.sc250) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c250,
          AVG(s.sc275) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c275,
          AVG(s.sc300) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c300,
          AVG(s.sc325) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c325,
          AVG(s.sc350) OVER (PARTITION BY s.series, s.model_name ORDER BY s.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c350
        FROM s
      )
      UPDATE %1$s t
         SET b125_cov = cv.c125, b150_cov = cv.c150, b175_cov = cv.c175, b200_cov = cv.c200, b225_cov = cv.c225,
             b250_cov = cv.c250, b275_cov = cv.c275, b300_cov = cv.c300, b325_cov = cv.c325, b350_cov = cv.c350
        FROM cov cv
       WHERE t.%2$I = cv.series AND t.model_name = cv.model_name AND t.%3$I = cv.yqm
    $u$, dest_qual, dest_series_col, dest_season_col) USING latest_id;

    -- CI selection (85/90/95)
    EXECUTE 'DROP TABLE IF EXISTS __ci_pick';
    EXECUTE format($u$
      CREATE TEMP TABLE __ci_pick AS
      WITH c AS (
        SELECT %1$I AS series, model_name, %2$I AS yqm,
               b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov
        FROM %3$s WHERE forecast_id = $1
        GROUP BY %1$I, model_name, %2$I, b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov
      ),
      sel AS (
        SELECT c.series, c.model_name, c.yqm,
               COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS x(i,cov)
                         WHERE cov >= 0.85 ORDER BY i LIMIT 1), 8) AS i85,
               COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS x(i,cov)
                         WHERE i > COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS y(i,cov)
                                             WHERE cov >= 0.85 ORDER BY i LIMIT 1), 8)
                           AND cov >= 0.90 ORDER BY i LIMIT 1),
                       LEAST(COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS y(i,cov)
                                       WHERE cov >= 0.85 ORDER BY i LIMIT 1), 8) + 1, 9)) AS i90,
               COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS x(i,cov)
                         WHERE i > LEAST(COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS y(i,cov)
                                                    WHERE cov >= 0.90 AND i > COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS z(i,cov)
                                                                                         WHERE cov >= 0.85 ORDER BY i LIMIT 1), 8)) ORDER BY i LIMIT 1), 9)
                           AND cov >= 0.95 ORDER BY i LIMIT 1),
                       LEAST(COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS y(i,cov)
                                       WHERE cov >= 0.90 AND i > COALESCE((SELECT i FROM (VALUES (1,c.b125_cov),(2,c.b150_cov),(3,c.b175_cov),(4,c.b200_cov),(5,c.b225_cov),(6,c.b250_cov),(7,c.b275_cov),(8,c.b300_cov),(9,c.b325_cov),(10,c.b350_cov)) AS z(i,cov)
                                                                           WHERE cov >= 0.85 ORDER BY i LIMIT 1), 8)) ORDER BY i LIMIT 1, 9) + 1, 10)) AS i95
        FROM c
      )
      SELECT s.series, s.model_name, s.yqm, s.i85, s.i90, s.i95
      FROM sel s
    $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;

    EXECUTE format($u$
      UPDATE %1$s t
         SET ci85_low =
               CASE cp.i85
                 WHEN 1 THEN t.fv_b125_l WHEN 2 THEN t.fv_b150_l WHEN 3 THEN t.fv_b175_l WHEN 4 THEN t.fv_b200_l WHEN 5 THEN t.fv_b225_l
                 WHEN 6 THEN t.fv_b250_l WHEN 7 THEN t.fv_b275_l WHEN 8 THEN t.fv_b300_l WHEN 9 THEN t.fv_b325_l WHEN 10 THEN t.fv_b350_l
               END,
             ci85_high =
               CASE cp.i85
                 WHEN 1 THEN t.fv_b125_u WHEN 2 THEN t.fv_b150_u WHEN 3 THEN t.fv_b175_u WHEN 4 THEN t.fv_b200_u WHEN 5 THEN t.fv_b225_u
                 WHEN 6 THEN t.fv_b250_u WHEN 7 THEN t.fv_b275_u WHEN 8 THEN t.fv_b300_u WHEN 9 THEN t.fv_b325_u WHEN 10 THEN t.fv_b350_u
               END,
             ci90_low =
               CASE cp.i90
                 WHEN 1 THEN t.fv_b125_l WHEN 2 THEN t.fv_b150_l WHEN 3 THEN t.fv_b175_l WHEN 4 THEN t.fv_b200_l WHEN 5 THEN t.fv_b225_l
                 WHEN 6 THEN t.fv_b250_l WHEN 7 THEN t.fv_b275_l WHEN 8 THEN t.fv_b300_l WHEN 9 THEN t.fv_b325_l WHEN 10 THEN t.fv_b350_l
               END,
             ci90_high =
               CASE cp.i90
                 WHEN 1 THEN t.fv_b125_u WHEN 2 THEN t.fv_b150_u WHEN 3 THEN t.fv_b175_u WHEN 4 THEN t.fv_b200_u WHEN 5 THEN t.fv_b225_u
                 WHEN 6 THEN t.fv_b250_u WHEN 7 THEN t.fv_b275_u WHEN 8 THEN t.fv_b300_u WHEN 9 THEN t.fv_b325_u WHEN 10 THEN t.fv_b350_u
               END,
             ci95_low =
               CASE cp.i95
                 WHEN 1 THEN t.fv_b125_l WHEN 2 THEN t.fv_b150_l WHEN 3 THEN t.fv_b175_l WHEN 4 THEN t.fv_b200_l WHEN 5 THEN t.fv_b225_l
                 WHEN 6 THEN t.fv_b250_l WHEN 7 THEN t.fv_b275_l WHEN 8 THEN t.fv_b300_l WHEN 9 THEN t.fv_b325_l WHEN 10 THEN t.fv_b350_l
               END,
             ci95_high =
               CASE cp.i95
                 WHEN 1 THEN t.fv_b125_u WHEN 2 THEN t.fv_b150_u WHEN 3 THEN t.fv_b175_u WHEN 4 THEN t.fv_b200_u WHEN 5 THEN t.fv_b225_u
                 WHEN 6 THEN t.fv_b250_u WHEN 7 THEN t.fv_b275_u WHEN 8 THEN t.fv_b300_u WHEN 9 THEN t.fv_b325_u WHEN 10 THEN t.fv_b350_u
               END
        FROM __ci_pick cp
       WHERE t.%2$I = cp.series AND t.model_name = cp.model_name AND t.%3$I = cp.yqm
    $u$, dest_qual, dest_series_col, dest_season_col) USING latest_id;

    -- A0/Ax comparisons + counts
    EXECUTE 'DROP TABLE IF EXISTS __a0_map';
    EXECUTE format($u$
      CREATE TEMP TABLE __a0_map AS
      SELECT base_model, %1$I AS yqm,
             MAX(fv_mape)      AS mape0,
             MAX(fv_mean_mape) AS mean_mape0,
             MAX(fv_mae)       AS mae0,
             MAX(fv_mean_mae)  AS mean_mae0,
             MAX(fv_rmse)      AS rmse0,
             MAX(fv_mean_rmse) AS mean_rmse0
      FROM %2$s WHERE forecast_id = $1 AND fmsr_series = 'A0'
      GROUP BY base_model, %1$I
    $u$, dest_season_col, dest_qual) USING latest_id;

    EXECUTE format($u$
      UPDATE %2$s t
         SET
           fv_variance = CASE
               WHEN t.value IS NULL
                 OR (CASE WHEN t.value > t.ci85_low AND t.value < t.ci85_high THEN 'Y' ELSE 'N' END) = 'Y'
               THEN NULL
               ELSE round( GREATEST( 0.0,
                               (t.value - t.ci85_high) / NULLIF(ABS(t.ci85_high),0),
                               (t.ci85_low - t.value) / NULLIF(ABS(t.ci85_low),0)
                            )::numeric, 4) END,
           mape_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mape IS NULL OR a0.mape0 IS NULL THEN NULL
                  WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H' END,
           mean_mape_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
                  WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H' END,
           accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mape IS NULL OR a0.mape0 IS NULL OR t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END,
           mae_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mae IS NULL OR a0.mae0 IS NULL THEN NULL
                  WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H' END,
           mean_mae_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
                  WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H' END,
           mae_accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mae IS NULL OR a0.mae0 IS NULL OR t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END,
           rmse_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_rmse IS NULL OR a0.rmse0 IS NULL THEN NULL
                  WHEN t.fv_rmse < a0.rmse0 THEN 'L' ELSE 'H' END,
           mean_rmse_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_mean_rmse IS NULL OR a0.mean_rmse0 IS NULL THEN NULL
                  WHEN t.fv_mean_rmse < a0.mean_rmse0 THEN 'L' ELSE 'H' END,
           rmse_accuracy_comparison =
             CASE WHEN t.fmsr_series = 'A0' THEN NULL
                  WHEN t.fv_rmse IS NULL OR a0.rmse0 IS NULL OR t.fv_mean_rmse IS NULL OR a0.mean_rmse0 IS NULL THEN NULL
                  WHEN (CASE WHEN t.fv_rmse < a0.rmse0 THEN 'L' ELSE 'H' END) = 'L'
                   AND (CASE WHEN t.fv_mean_rmse < a0.mean_rmse0 THEN 'L' ELSE 'H' END) = 'L' THEN 'Y' ELSE 'N' END
      FROM __a0_map a0
     WHERE t.base_model = a0.base_model
       AND t.%1$I       = a0.yqm
    $u$, dest_season_col, dest_qual) USING latest_id;

    -- Lagged counts
    EXECUTE format($u$ WITH flags AS (
        SELECT
          %1$I AS series, model_name, %2$I AS yqm,
          MAX(CASE WHEN accuracy_comparison = 'Y' THEN 1 ELSE 0 END) AS acc_mape_y,
          MAX(CASE WHEN mae_accuracy_comparison  = 'Y' THEN 1 ELSE 0 END) AS acc_mae_y,
          MAX(CASE WHEN rmse_accuracy_comparison = 'Y' THEN 1 ELSE 0 END) AS acc_rmse_y
        FROM %3$s WHERE forecast_id = $1
        GROUP BY %1$I, model_name, %2$I
      ),
      j AS (
        SELECT f.series, f.model_name, f.yqm, sd.season_start, f.acc_mape_y, f.acc_mae_y, f.acc_rmse_y
        FROM flags f
        JOIN __season_dim sd ON sd.series = f.series AND sd.model_name = f.model_name AND sd.yqm = f.yqm
      ),
      stats AS (
        SELECT j.series, j.model_name, j.yqm,
               SUM(j.acc_mape_y) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mape_cnt,
               SUM(j.acc_mae_y)  OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mae_cnt,
               SUM(j.acc_rmse_y) OVER (PARTITION BY j.series, j.model_name ORDER BY j.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_rmse_cnt
        FROM j
      )
      UPDATE %3$s t
         SET best_mape_count = COALESCE(st.best_mape_cnt,0),
             best_mae_count  = COALESCE(st.best_mae_cnt,0),
             best_rmse_count = COALESCE(st.best_rmse_cnt,0)
        FROM stats st
       WHERE t.%1$I = st.series AND t.model_name = st.model_name AND t.%2$I = st.yqm
    $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;

    -- Clamp
    EXECUTE format($c$ UPDATE %s SET
        value            = CASE WHEN value IS NULL THEN NULL ELSE round(value::numeric, 4) END,
        base_fv          = CASE WHEN base_fv IS NULL THEN NULL ELSE round(base_fv::numeric, 4) END,
        fmsr_value       = CASE WHEN fmsr_value IS NULL THEN NULL ELSE round(fmsr_value::numeric, 4) END,
        fv               = CASE WHEN fv IS NULL THEN NULL ELSE round(fv::numeric, 4) END,
        fv_error         = CASE WHEN fv_error IS NULL THEN NULL ELSE round(fv_error::numeric, 4) END,
        fv_mape          = CASE WHEN fv_mape IS NULL THEN NULL ELSE round(fv_mape::numeric, 4) END,
        fv_mean_mape     = CASE WHEN fv_mean_mape IS NULL THEN NULL ELSE round(fv_mean_mape::numeric, 4) END,
        fv_mean_mape_c   = CASE WHEN fv_mean_mape_c IS NULL THEN NULL ELSE round(fv_mean_mape_c::numeric, 4) END,
        fv_mae           = CASE WHEN fv_mae IS NULL THEN NULL ELSE round(fv_mae::numeric, 4) END,
        fv_mean_mae      = CASE WHEN fv_mean_mae IS NULL THEN NULL ELSE round(fv_mean_mae::numeric, 4) END,
        fv_mean_mae_c    = CASE WHEN fv_mean_mae_c IS NULL THEN NULL ELSE round(fv_mean_mae_c::numeric, 4) END,
        fv_rmse          = CASE WHEN fv_rmse IS NULL THEN NULL ELSE round(fv_rmse::numeric, 4) END,
        fv_mean_rmse     = CASE WHEN fv_mean_rmse IS NULL THEN NULL ELSE round(fv_mean_rmse::numeric, 4) END,
        fv_mean_rmse_c   = CASE WHEN fv_mean_rmse_c IS NULL THEN NULL ELSE round(fv_mean_rmse_c::numeric, 4) END,
        fv_b125_u        = CASE WHEN fv_b125_u IS NULL THEN NULL ELSE round(fv_b125_u::numeric, 4) END,
        fv_b125_l        = CASE WHEN fv_b125_l IS NULL THEN NULL ELSE round(fv_b125_l::numeric, 4) END,
        fv_b150_u        = CASE WHEN fv_b150_u IS NULL THEN NULL ELSE round(fv_b150_u::numeric, 4) END,
        fv_b150_l        = CASE WHEN fv_b150_l IS NULL THEN NULL ELSE round(fv_b150_l::numeric, 4) END,
        fv_b175_u        = CASE WHEN fv_b175_u IS NULL THEN NULL ELSE round(fv_b175_u::numeric, 4) END,
        fv_b175_l        = CASE WHEN fv_b175_l IS NULL THEN NULL ELSE round(fv_b175_l::numeric, 4) END,
        fv_b200_u        = CASE WHEN fv_b200_u IS NULL THEN NULL ELSE round(fv_b200_u::numeric, 4) END,
        fv_b200_l        = CASE WHEN fv_b200_l IS NULL THEN NULL ELSE round(fv_b200_l::numeric, 4) END,
        fv_b225_u        = CASE WHEN fv_b225_u IS NULL THEN NULL ELSE round(fv_b225_u::numeric, 4) END,
        fv_b225_l        = CASE WHEN fv_b225_l IS NULL THEN NULL ELSE round(fv_b225_l::numeric, 4) END,
        fv_b250_u        = CASE WHEN fv_b250_u IS NULL THEN NULL ELSE round(fv_b250_u::numeric, 4) END,
        fv_b250_l        = CASE WHEN fv_b250_l IS NULL THEN NULL ELSE round(fv_b250_l::numeric, 4) END,
        fv_b275_u        = CASE WHEN fv_b275_u IS NULL THEN NULL ELSE round(fv_b275_u::numeric, 4) END,
        fv_b275_l        = CASE WHEN fv_b275_l IS NULL THEN NULL ELSE round(fv_b275_l::numeric, 4) END,
        fv_b300_u        = CASE WHEN fv_b300_u IS NULL THEN NULL ELSE round(fv_b300_u::numeric, 4) END,
        fv_b300_l        = CASE WHEN fv_b300_l IS NULL THEN NULL ELSE round(fv_b300_l::numeric, 4) END,
        fv_b325_u        = CASE WHEN fv_b325_u IS NULL THEN NULL ELSE round(fv_b325_u::numeric, 4) END,
        fv_b325_l        = CASE WHEN fv_b325_l IS NULL THEN NULL ELSE round(fv_b325_l::numeric, 4) END,
        fv_b350_u        = CASE WHEN fv_b350_u IS NULL THEN NULL ELSE round(fv_b350_u::numeric, 4) END,
        fv_b350_l        = CASE WHEN fv_b350_l IS NULL THEN NULL ELSE round(fv_b350_l::numeric, 4) END,
        b125_cov         = CASE WHEN b125_cov IS NULL THEN NULL ELSE round(b125_cov::numeric, 4) END,
        b150_cov         = CASE WHEN b150_cov IS NULL THEN NULL ELSE round(b150_cov::numeric, 4) END,
        b175_cov         = CASE WHEN b175_cov IS NULL THEN NULL ELSE round(b175_cov::numeric, 4) END,
        b200_cov         = CASE WHEN b200_cov IS NULL THEN NULL ELSE round(b200_cov::numeric, 4) END,
        b225_cov         = CASE WHEN b225_cov IS NULL THEN NULL ELSE round(b225_cov::numeric, 4) END,
        b250_cov         = CASE WHEN b250_cov IS NULL THEN NULL ELSE round(b250_cov::numeric, 4) END,
        b275_cov         = CASE WHEN b275_cov IS NULL THEN NULL ELSE round(b275_cov::numeric, 4) END,
        b300_cov         = CASE WHEN b300_cov IS NULL THEN NULL ELSE round(b300_cov::numeric, 4) END,
        b325_cov         = CASE WHEN b325_cov IS NULL THEN NULL ELSE round(b325_cov::numeric, 4) END,
        b350_cov         = CASE WHEN b350_cov IS NULL THEN NULL ELSE round(b350_cov::numeric, 4) END,
        ci85_low         = CASE WHEN ci85_low IS NULL THEN NULL ELSE round(ci85_low::numeric, 4) END,
        ci85_high        = CASE WHEN ci85_high IS NULL THEN NULL ELSE round(ci85_high::numeric, 4) END,
        ci90_low         = CASE WHEN ci90_low IS NULL THEN NULL ELSE round(ci90_low::numeric, 4) END,
        ci90_high        = CASE WHEN ci90_high IS NULL THEN NULL ELSE round(ci90_high::numeric, 4) END,
        ci95_low         = CASE WHEN ci95_low IS NULL THEN NULL ELSE round(ci95_low::numeric, 4) END,
        ci95_high        = CASE WHEN ci95_high IS NULL THEN NULL ELSE round(ci95_high::numeric, 4) END,
        fv_variance      = CASE WHEN fv_variance IS NULL THEN NULL ELSE round(fv_variance::numeric, 4) END,
        fv_variance_mean = CASE WHEN fv_variance_mean IS NULL THEN NULL ELSE round(fv_variance_mean::numeric, 4) END
    $c$, dest_qual) USING latest_id;

    -- Commit work to destination
    PERFORM set_config('synchronous_commit','off',true);
    EXECUTE format('DELETE FROM %s WHERE forecast_id = $1', dest_real_qual) USING latest_id;
    EXECUTE 'INSERT INTO '||dest_real_qual||' SELECT * FROM __work WHERE forecast_id = $1' USING latest_id;

    RAISE NOTICE 'COMPLETE series: %', dest_rel;
  END LOOP;

  RAISE NOTICE 'ALL DONE (elapsed %.3f s)', EXTRACT(epoch FROM clock_timestamp() - t_run_start);
END
$$;

GRANT EXECUTE ON FUNCTION engine.msqm_forecast(text) TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.msqm_forecast__core() TO matrix_reader, tsf_engine_app;