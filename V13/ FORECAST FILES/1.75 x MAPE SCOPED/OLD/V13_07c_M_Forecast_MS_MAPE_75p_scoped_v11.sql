-- 2025-09-26 SCOPED v11: Dest writes gated by (forecast_id, date) via __keys; strict scoping; no math/logic changes.
-- 2025-09-25 10:29:44 FIX: registry status columns -> ms_complete/msq_complete/msqm_complete (was *_completed). No other changes.
-- V12_07a/08a/09a (2025-09-24) — Ambiguity fix
-- PATCH: Use v_forecast_name variable inside core to avoid column/variable ambiguity.

-- Entry point: engine.ms_forecast(forecast_name TEXT)
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

-- Core body
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
  -- Resolve forecast_target_id from provided name
  v_forecast_name := current_setting('engine.forecast_name', true);
  IF v_forecast_name IS NULL THEN
    RAISE EXCEPTION 'engine.ms_forecast(...) requires forecast_name';
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
    -- v11 prelude: per-run temp subsets + composite key domain (forecast_id, date)
    CREATE TEMP TABLE __ih_subset ON COMMIT DROP AS
    SELECT * FROM engine.instance_historical WHERE forecast_id = latest_id;
    CREATE INDEX ON __ih_subset (date);

    CREATE TEMP TABLE __ms_subset ON COMMIT DROP AS
    SELECT * FROM engine.wd_md_instance_forecast_ms WHERE forecast_id = latest_id;
    CREATE INDEX ON __ms_subset (date);

    CREATE TEMP TABLE __msq_subset ON COMMIT DROP AS
    SELECT * FROM engine.wd_md_instance_forecast_msq WHERE forecast_id = latest_id;
    CREATE INDEX ON __msq_subset (date);

    CREATE TEMP TABLE __msqm_subset ON COMMIT DROP AS
    SELECT * FROM engine.wd_md_instance_forecast_msqm WHERE forecast_id = latest_id;
    CREATE INDEX ON __msqm_subset (date);

    CREATE TEMP TABLE __keys (
        forecast_id uuid NOT NULL,
        date date NOT NULL,
        PRIMARY KEY (forecast_id, date)
    ) ON COMMIT DROP;
    INSERT INTO __keys (forecast_id, date)
    SELECT latest_id, date FROM (SELECT DISTINCT date FROM __ih_subset) d;

    ANALYZE __ih_subset; ANALYZE __ms_subset; ANALYZE __msq_subset; ANALYZE __msqm_subset; ANALYZE __keys;
    
PERFORM set_config('client_min_messages','NOTICE',true);
  RAISE NOTICE 'RUN START';

  -- best-effort session hints
  BEGIN
    PERFORM set_config('jit','off',true);
    PERFORM set_config('work_mem','256MB',true);
    PERFORM set_config('maintenance_work_mem','512MB',true);
    PERFORM set_config('max_parallel_workers_per_gather','4',true);
    PERFORM set_config('parallel_setup_cost','0',true);
    PERFORM set_config('parallel_tuple_cost','0',true);
    PERFORM set_config('synchronous_commit','off',true);
    PERFORM set_config('temp_buffers','64MB',true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Session tuning skipped: %', SQLERRM;
  END;

  -- Purge any prior scratch tables once per run
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

    /* latest_id discovery removed: will use forecast_target_id */

    EXECUTE $q$
      select (min(date) + interval '2 years')::date
      from __ih_subset
      where forecast_id = $1
    $q$
    USING latest_id
    INTO start_from;
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
          fv_u numeric,
          fv_l numeric,
          mape_comparison text,
          mean_mape_comparison text,
          accuracy_comparison text,
          best_fm_count integer,
          best_fm_odds numeric,
          best_fm_sig numeric,
          fv_interval text,
          fv_interval_c integer,
          fv_interval_odds numeric,
          fv_interval_sig numeric,
          fv_variance numeric,
          fv_variance_mean numeric,
          created_at timestamptz DEFAULT now(),
          PRIMARY KEY (forecast_id, date, model_name, fmsr_series)
        )
      $ct$, dest_qual);
    END IF;

    -- Determine destination column names
    dest_series_col := 'series';
    dest_season_col := 'season';
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_series_col;
    IF NOT FOUND THEN dest_series_col := base; END IF;
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_season_col;
    IF NOT FOUND THEN dest_season_col := base || '_yqm'; END IF;

    sr_base_col       := 'sr.' || quote_ident(base);
    sr_yqm_col        := 'sr.' || quote_ident(base || '_yqm');
    sr_fmsr_a1_col    := 'sr.' || quote_ident(base || '_fmsr_a1');
    sr_fmsr_a2_col    := 'sr.' || quote_ident(base || '_fmsr_a2');
    sr_fmsr_a2w_col   := 'sr.' || quote_ident(base || '_fmsr_a2w');
    sr_fmsr_a3_col    := 'sr.' || quote_ident(base || '_fmsr_a3');
    sr_fmsr_a3w_col   := 'sr.' || quote_ident(base || '_fmsr_a3w');

    -- PASS 1 — CTAS
    t_pass_start := clock_timestamp();
    RAISE NOTICE 'PASS 1';

    EXECUTE 'drop table if exists __tmp_forecast_build';
    sql := format($f$
      create temporary table __tmp_forecast_build as
      with variants as (
        values
          ('LMM1','lmm1','A0'), ('LMM1','lmm1','A1'), ('LMM1','lmm1','A2'), ('LMM1','lmm1','A2W'), ('LMM1','lmm1','A3'), ('LMM1','lmm1','A3W'),
          ('LMM5','lmm5','A0'), ('LMM5','lmm5','A1'), ('LMM5','lmm5','A2'), ('LMM5','lmm5','A2W'), ('LMM5','lmm5','A3'), ('LMM5','lmm5','A3W'),
          ('LMM10','lmm10','A0'), ('LMM10','lmm10','A1'), ('LMM10','lmm10','A2'), ('LMM10','lmm10','A2W'), ('LMM10','lmm10','A3'), ('LMM10','lmm10','A3W'),
          ('LMM15','lmm15','A0'), ('LMM15','lmm15','A1'), ('LMM15','lmm15','A2'), ('LMM15','lmm15','A2W'), ('LMM15','lmm15','A3'), ('LMM15','lmm15','A3W'),
          ('LMM30','lmm30','A0'), ('LMM30','lmm30','A1'), ('LMM30','lmm30','A2'), ('LMM30','lmm30','A2W'), ('LMM30','lmm30','A3'), ('LMM30','lmm30','A3W'),
          ('ARIMA_M','arima_m','A0'), ('ARIMA_M','arima_m','A1'), ('ARIMA_M','arima_m','A2'), ('ARIMA_M','arima_m','A2W'), ('ARIMA_M','arima_m','A3'), ('ARIMA_M','arima_m','A3W'),
          ('SES_M','ses_m','A0'), ('SES_M','ses_m','A1'), ('SES_M','ses_m','A2'), ('SES_M','ses_m','A2W'), ('SES_M','ses_m','A3'), ('SES_M','ses_m','A3W'),
          ('HWES_M','hwes_m','A0'), ('HWES_M','hwes_m','A1'), ('HWES_M','hwes_m','A2'), ('HWES_M','hwes_m','A2W'), ('HWES_M','hwes_m','A3'), ('HWES_M','hwes_m','A3W')
      )
      select
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
        NULL::numeric as fv_error,
        NULL::numeric as fv_mape,
        NULL::numeric as fv_mean_mape,
        NULL::numeric as fv_mean_mape_c,
        NULL::numeric as fv_u,
        NULL::numeric as fv_l,
        NULL::text    as mape_comparison,
        NULL::text    as mean_mape_comparison,
        NULL::text    as accuracy_comparison,
        NULL::int     as best_fm_count,
        NULL::numeric as best_fm_odds,
        NULL::numeric as best_fm_sig,
        NULL::text    as fv_interval,
        NULL::numeric as fv_interval_c,
        NULL::numeric as fv_interval_odds,
        NULL::numeric as fv_interval_sig,
        NULL::numeric as fv_variance,
        NULL::numeric as fv_variance_mean
      from %s sr
      join __ih_subset h
        on h.forecast_id = sr.forecast_id
       and h.date        = sr.date
      cross join variants v
      where sr.forecast_id = %L
      order by sr.date, v.column1, v.column3;
    
    DELETE FROM __tmp_forecast_build WHERE base_fv IS NULL;
    UPDATE __tmp_forecast_build
       SET fv_error = ABS(value - fv);
    ANALYZE __tmp_forecast_build;
$f$,
      sr_base_col, 'series',
      sr_yqm_col,  base || '_yqm',
      base,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      h_lmm1, h_lmm5, h_lmm10, h_lmm15, h_lmm30, h_arima_m, h_ses_m, h_hwes_m,
      sr_fmsr_a1_col, sr_fmsr_a2_col, sr_fmsr_a2w_col, sr_fmsr_a3_col, sr_fmsr_a3w_col,
      sr_qual, latest_id
    );
    EXECUTE sql;

    EXECUTE format($i$ INSERT INTO %1$s (
        forecast_id, "date", value, %2$I, %3$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, fv_error, fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_u, fv_l, mape_comparison, mean_mape_comparison, accuracy_comparison,
        best_fm_count, best_fm_odds, best_fm_sig, fv_interval, fv_interval_c,
        fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, created_at
      )
      SELECT
        forecast_id, "date", value, %4$I, %5$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, ABS(value - fv)::numeric AS fv_error, fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_u, fv_l, mape_comparison, mean_mape_comparison, accuracy_comparison,
        best_fm_count, best_fm_odds, best_fm_sig, fv_interval, fv_interval_c,
        fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, now()
      FROM __tmp_forecast_build
      ON CONFLICT (forecast_id, date, model_name, fmsr_series) DO NOTHING $i$, dest_qual, dest_series_col, dest_season_col, 'series', base || '_yqm') USING latest_id;

    GET DIAGNOSTICS rcnt = ROW_COUNT;
    RAISE NOTICE 'PASS 1 — inserted rows: %', rcnt;

    IF enable_full_analyze THEN
      EXECUTE format('ANALYZE %s', dest_qual);
    END IF;

    -- PASS 3 — fv_mape
    RAISE NOTICE 'PASS 3 — fv_mape';
    EXECUTE format($u$ WITH m AS (
        SELECT %1$I AS series, model_name, %2$I AS yqm, AVG(ABS(value - fv) / NULLIF(ABS(value),0))::numeric AS mape
        FROM %3$s WHERE forecast_id = $1 AND  value IS NOT NULL AND fv IS NOT NULL AND ABS(value) > 0
        GROUP BY %1$I, model_name, %2$I
      )
      UPDATE %3$s t
         SET fv_mape = m.mape
        FROM m
       WHERE t.%1$I       = m.series
         AND t.model_name = m.model_name
         AND t.%2$I       = m.yqm $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- season_dim temp
    RAISE NOTICE 'BUILD __season_dim';
    EXECUTE 'DROP TABLE IF EXISTS __season_dim';
    EXECUTE format($u$ CREATE TEMPORARY TABLE __season_dim AS
      SELECT
        %1$I       AS series,
        model_name AS model_name,
        %2$I       AS yqm,
        MIN(date)  AS season_start,
        MAX(fv_mape) AS season_mape
      FROM %3$s WHERE forecast_id = $1 AND  base_fv IS NOT NULL
      GROUP BY %1$I, model_name, %2$I $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __season_dim (series, model_name, yqm);
    ANALYZE __season_dim;

    -- index to support 6A
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %s (%I, model_name, %I)', dest_rel || '_sm_yqm', dest_qual, dest_series_col, dest_season_col);
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- PASS 4a — fv_mean_mape & fv_mean_mape_c
    RAISE NOTICE 'PASS 4a — fv_mean_mape/ count';
    EXECUTE format($u$ WITH stats AS (
        SELECT
          s.series,
          s.model_name,
          s.yqm,
          SUM(s.season_mape) OVER (
            PARTITION BY s.series, s.model_name
            ORDER BY s.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_sum,
          COUNT(s.season_mape) OVER (
            PARTITION BY s.series, s.model_name
            ORDER BY s.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_cnt
        FROM __season_dim s
      )
      UPDATE %1$s t
         SET fv_mean_mape   = (st.prev_sum / NULLIF(st.prev_cnt,0)),
             fv_mean_mape_c = (st.prev_cnt)::numeric
        FROM stats st
       WHERE t.%2$I       = st.series
         AND t.model_name = st.model_name
         AND t.%3$I       = st.yqm
         AND (
              t.fv_mean_mape   IS DISTINCT FROM (st.prev_sum / NULLIF(st.prev_cnt,0)) OR
              t.fv_mean_mape_c IS DISTINCT FROM (st.prev_cnt)::numeric
         ) $u$, dest_qual, dest_series_col, dest_season_col) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- PASS 4b — bands
    EXECUTE format($u$ UPDATE %1$s
         SET fv_u = fv + ((fv * fv_mean_mape) * 1.75),
             fv_l = GREATEST(0, fv - ((fv * fv_mean_mape) * 1.75))
       WHERE fv_mean_mape IS NOT NULL $u$, dest_qual) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- A0 cache temp
    RAISE NOTICE 'BUILD __a0_map';
    EXECUTE 'DROP TABLE IF EXISTS __a0_map';
    EXECUTE format($u$ CREATE TEMPORARY TABLE __a0_map AS
      SELECT base_model, %1$I AS yqm,
             MAX(fv_mape)      AS mape0,
             MAX(fv_mean_mape) AS mean_mape0
      FROM %2$s WHERE forecast_id = $1 AND  fmsr_series = 'A0'
      GROUP BY base_model, %1$I $u$, dest_season_col, dest_qual) USING latest_id;
    CREATE INDEX ON __a0_map (base_model, yqm);
    ANALYZE __a0_map;

    -- PASS 5 — intervals / variance / comparisons
    RAISE NOTICE 'PASS 5 — intervals / comparisons';
    EXECUTE format($u$ UPDATE %2$s t
         SET
           fv_interval =
             CASE
               WHEN t.value IS NULL OR t.fv_l IS NULL OR t.fv_u IS NULL THEN NULL
               WHEN t.value > t.fv_l AND t.value < t.fv_u THEN 'Y' ELSE 'N'
             END,
           fv_variance = CASE
               WHEN t.value IS NULL
                 OR (CASE WHEN t.value > t.fv_l AND t.value < t.fv_u THEN 'Y' ELSE 'N' END) = 'Y'
               THEN NULL
               ELSE round( GREATEST( 0.0,
                               (t.value - t.fv_u) / NULLIF(ABS(t.fv_u),0),
                               (t.fv_l - t.value) / NULLIF(ABS(t.fv_l),0)
                            )::numeric, 4) END,
           mape_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mape IS NULL OR a0.mape0 IS NULL THEN NULL
               WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H'
             END,
           mean_mape_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
               WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H'
             END,
           accuracy_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mape IS NULL OR a0.mape0 IS NULL OR t.fv_mean_mape IS NULL OR a0.mean_mape0 IS NULL THEN NULL
               WHEN (CASE WHEN t.fv_mape < a0.mape0 THEN 'L' ELSE 'H' END) = 'L'
                AND (CASE WHEN t.fv_mean_mape < a0.mean_mape0 THEN 'L' ELSE 'H' END) = 'L'
               THEN 'Y' ELSE 'N'
             END
        FROM __a0_map a0
       WHERE t.base_model = a0.base_model
         AND t.%1$I       = a0.yqm $u$, dest_season_col, dest_qual) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- PASS 6A — lagged counts & variance mean (fv_interval_c rule)
    RAISE NOTICE 'PASS 6A — lagged counts';
    EXECUTE format($u$ WITH flags AS (
        SELECT
          %1$I AS series,
          model_name,
          %2$I AS yqm,
          MAX(CASE WHEN accuracy_comparison = 'Y' THEN 1 ELSE 0 END) AS acc_y,
          /* Season passes if: 4+ rows → Yes/Total ≥ 0.75; 1–3 rows → all Yes. Treat exactly 75%% as a pass. */
          CASE
            WHEN COUNT(fv_interval) >= 4
              THEN CASE
                     WHEN (SUM(CASE WHEN fv_interval = 'Y' THEN 1 ELSE 0 END)::numeric
                           / NULLIF(COUNT(fv_interval)::numeric, 0)) >= 0.75
                     THEN 1 ELSE 0 END
            WHEN COUNT(fv_interval) BETWEEN 1 AND 3
              THEN CASE
                     WHEN SUM(CASE WHEN fv_interval = 'Y' THEN 1 ELSE 0 END) = COUNT(fv_interval)
                     THEN 1 ELSE 0 END
            ELSE 0
          END AS int_y,
          AVG(CASE WHEN fv_variance IS NOT NULL THEN fv_variance END) AS season_var
        FROM %3$s WHERE forecast_id = $1
        GROUP BY %1$I, model_name, %2$I
      ),
      j AS (
        SELECT f.series, f.model_name, f.yqm, sd.season_start, f.acc_y, f.int_y, f.season_var
        FROM flags f
        JOIN __season_dim sd
          ON sd.series = f.series AND sd.model_name = f.model_name AND sd.yqm = f.yqm
      ),
      stats AS (
        SELECT
          j.series,
          j.model_name,
          j.yqm,
          SUM(j.acc_y) OVER (
            PARTITION BY j.series, j.model_name
            ORDER BY j.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_best_cnt,
          SUM(j.int_y) OVER (
            PARTITION BY j.series, j.model_name
            ORDER BY j.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_int_cnt,
          SUM(j.season_var) OVER (
            PARTITION BY j.series, j.model_name
            ORDER BY j.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS cum_var_sum,
          COUNT(j.season_var) OVER (
            PARTITION BY j.series, j.model_name
            ORDER BY j.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS cum_var_cnt
        FROM j
      )
      UPDATE %3$s t
         SET best_fm_count    = COALESCE(st.prev_best_cnt, 0),
             fv_interval_c    = COALESCE(st.prev_int_cnt, 0),
             fv_variance_mean = round( (st.cum_var_sum / NULLIF(st.cum_var_cnt,0))::numeric, 4 )
        FROM stats st
       WHERE t.%1$I       = st.series
         AND t.model_name = st.model_name
         AND t.%2$I       = st.yqm
         AND (
              t.best_fm_count IS DISTINCT FROM COALESCE(st.prev_best_cnt, 0) OR
              t.fv_interval_c IS DISTINCT FROM COALESCE(st.prev_int_cnt, 0) OR
              t.fv_variance_mean IS DISTINCT FROM round( (st.cum_var_sum / NULLIF(st.cum_var_cnt,0))::numeric, 4 )
         )
         AND (
              t.best_fm_count IS DISTINCT FROM COALESCE(st.prev_best_cnt, 0) OR
              t.fv_interval_c IS DISTINCT FROM COALESCE(st.prev_int_cnt, 0) OR
              t.fv_variance_mean IS NULL
           ) $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- PASS 6B — odds & significance using engine.binom_p
    RAISE NOTICE 'PASS 6B — odds/significance';
    EXECUTE format($u$ WITH stats AS (
        SELECT
          %1$I AS series,
          model_name,
          %2$I AS yqm,
          COALESCE(fv_mean_mape_c,0)::int AS n,
          COALESCE(best_fm_count,0)::int AS k_best,
          COALESCE(fv_interval_c,0)::int AS k_int
        FROM %3$s WHERE forecast_id = $1
      ),
      best AS (
        SELECT s.*,
               CASE WHEN s.n <= 0 THEN NULL ELSE (s.k_best::double precision / NULLIF(s.n::double precision,0)) END AS odds_best
        FROM stats s
      ),
      best_p AS (
        SELECT b.series, b.model_name, b.yqm,
               b.odds_best,
               p.p_two_sided AS p_best,
               b.k_int, b.n, b.k_best
        FROM best b
        LEFT JOIN engine.binom_p p
               ON p.n = b.n AND p.k = b.k_best
      ),
      ints AS (
        SELECT
          bp.*,
          CASE WHEN bp.n <= 0 THEN NULL ELSE (bp.k_int::double precision / NULLIF(bp.n::double precision,0)) END AS odds_int
        FROM best_p bp
      ),
      ints_p AS (
        SELECT i.series, i.model_name, i.yqm,
               i.odds_best,
               i.p_best,
               p2.p_two_sided AS p_int,
               i.odds_int
        FROM ints i
        LEFT JOIN engine.binom_p p2
               ON p2.n = i.n AND p2.k = i.k_int
      )
      UPDATE %3$s t
         SET best_fm_odds     = ip.odds_best,
             best_fm_sig      = CASE WHEN ip.p_best IS NULL THEN NULL ELSE ip.p_best::numeric END,
             fv_interval_odds = ip.odds_int,
             fv_interval_sig  = CASE WHEN ip.p_int IS NULL THEN NULL ELSE ip.p_int::numeric END
        FROM ints_p ip
       WHERE t.%1$I       = ip.series
         AND t.model_name = ip.model_name
         AND t.%2$I       = ip.yqm $u$, dest_series_col, dest_season_col, dest_qual) USING latest_id;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;

    -- PASS 7 — clamp to 4dp
    RAISE NOTICE 'PASS 7 — clamp to 4dp';
    EXECUTE format($c$ UPDATE %s SET
        value            = round(value::numeric, 4),
        base_fv          = round(base_fv::numeric, 4),
        fmsr_value       = round(fmsr_value::numeric, 4),
        fv               = round(fv::numeric, 4),
        fv_error         = CASE WHEN fv_error IS NULL THEN NULL ELSE round(fv_error::numeric, 4) END,
        fv_mape           = CASE WHEN fv_mape IS NULL THEN NULL ELSE round(fv_mape::numeric, 4) END,
        fv_mean_mape      = CASE WHEN fv_mean_mape IS NULL THEN NULL ELSE round(fv_mean_mape::numeric, 4) END,
        fv_u             = CASE WHEN fv_u IS NULL THEN NULL ELSE round(fv_u::numeric, 4) END,
        fv_l             = CASE WHEN fv_l IS NULL THEN NULL ELSE round(fv_l::numeric, 4) END,
        best_fm_odds     = CASE WHEN best_fm_odds IS NULL THEN NULL ELSE round(best_fm_odds::numeric, 4) END,
        best_fm_sig      = CASE WHEN best_fm_sig IS NULL THEN NULL ELSE round(best_fm_sig::numeric, 4) END,
        fv_interval_odds = CASE WHEN fv_interval_odds IS NULL THEN NULL ELSE round(fv_interval_odds::numeric, 4) END,
        fv_interval_sig  = CASE WHEN fv_interval_sig IS NULL THEN NULL ELSE round(fv_interval_sig::numeric, 4) END,
        fv_variance      = CASE WHEN fv_variance IS NULL THEN NULL ELSE round(fv_variance::numeric, 4) END,
        fv_variance_mean = CASE WHEN fv_variance_mean IS NULL THEN NULL ELSE round(fv_variance_mean::numeric, 4) END $c$, dest_qual) USING latest_id;

    -- enforce integer types
    /* DISABLED (view-safe hotfix):
EXECUTE format(
      'ALTER TABLE %s
         ALTER COLUMN best_fm_count    TYPE integer USING COALESCE(best_fm_count,0)::integer,
         ALTER COLUMN fv_interval_c    TYPE integer USING COALESCE(fv_interval_c,0)::integer,
         ALTER COLUMN fv_mean_mape_c    TYPE integer USING COALESCE(fv_mean_mape_c,0)::integer',
      dest_qual
    );

    
*/
RAISE NOTICE 'COMPLETE series: % (elapsed %.3f s)',
      dest_rel, EXTRACT(epoch FROM clock_timestamp() - t_series_start);
  END LOOP;

  
  -- Mark completion in forecast_registry (ms_complete) — type-aware
  BEGIN
    PERFORM 1
    FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='ms_complete';

    IF FOUND THEN
      -- timestamp-like types
      PERFORM 1 FROM information_schema.columns
        WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='ms_complete'
          AND data_type IN ('timestamp without time zone','timestamp with time zone','date','time without time zone','time with time zone');
      IF FOUND THEN
        EXECUTE format('UPDATE engine.forecast_registry SET ms_complete = now() WHERE forecast_id = $1') USING latest_id;
      ELSE
        -- boolean
        PERFORM 1 FROM information_schema.columns
          WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='ms_complete' AND data_type IN ('boolean');
        IF FOUND THEN
          EXECUTE format('UPDATE engine.forecast_registry SET ms_complete = true WHERE forecast_id = $1') USING latest_id;
        ELSE
          -- default to text
          EXECUTE format('UPDATE engine.forecast_registry SET ms_complete = %s WHERE forecast_id = $1', quote_literal('complete')) USING latest_id;
        END IF;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Registry update for ms_complete failed: %', SQLERRM;
  END;

  -- Mark registry as complete (text)
  EXECUTE format('UPDATE engine.forecast_registry SET ms_complete = %s WHERE forecast_id = $1', quote_literal('complete'))
  USING latest_id;
  RAISE NOTICE 'ALL DONE (total elapsed %.3f s)',
    EXTRACT(epoch FROM clock_timestamp() - t_run_start);
END


$$;

GRANT EXECUTE ON FUNCTION engine.ms_forecast(text) TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.ms_forecast__core() TO matrix_reader, tsf_engine_app;
