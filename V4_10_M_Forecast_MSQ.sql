-- VC V4.10.2 (2025-09-17): FIX — updated binomial source to engine.binom_p everywhere; no other changes.
-- DO NOT EDIT BELOW THIS LINE.

-- VC V4.10.1 (2025-09-17): Clean pass — removed stray characters; no logic changes.
-- File name unchanged; functions remain engine.build_forecast_msq() and engine.build_forecast_msq_core().

-- V4_10_M_Forecast_MSQ.sql
-- VC V4.10 (2025-09-17): Wrapper/headers pattern from V4_09; ONE core change to CREATE TABLE if missing; binom source updated to engine.binom_p.
-- Functions: engine.build_forecast_msq() [wrapper], engine.build_forecast_msq_core() [core].


BEGIN;
DROP FUNCTION IF EXISTS engine.build_forecast_msq();
DROP FUNCTION IF EXISTS engine.build_forecast_msq(uuid);
DROP FUNCTION IF EXISTS engine.build_forecast_msq(uuid, uuid);
DROP FUNCTION IF EXISTS engine.build_forecast_msq_core();
DROP FUNCTION IF EXISTS engine.build_forecast_msq_core(uuid);
-- legacy
DROP FUNCTION IF EXISTS engine.build_forecast_ms_sq();
DROP FUNCTION IF EXISTS engine.build_forecast_ms_sq(uuid);
DROP FUNCTION IF EXISTS engine.build_forecast_ms_sq(uuid, uuid);
COMMIT;

CREATE OR REPLACE FUNCTION engine.build_forecast_msq_core()
RETURNS void
AS $$


DECLARE
  -- ====== TOGGLES (A) ======
  enable_extended_stats   boolean := false;  -- ON to (re)create extended stats per table
  enable_cluster_vacuum   boolean := false;  -- ON to CLUSTER/VACUUM at end of each table
  enable_full_analyze     boolean := true;   -- ON to run ANALYZE after each major step
  enable_binom_build      boolean := true;   -- ON to refresh binomial caches for needed (n) and (n,k)

  -- timing (H)
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

  last_value_date   date;
  cutoff_date       date;

  sql               text;
  idx_tag           text;
  idx_date_name     text;

  rcnt              bigint;

  -- no-cache helpers
  id_key  text;
  tname   text;
BEGIN
  PERFORM set_config('client_min_messages','NOTICE',true);
  RAISE NOTICE '[%] RUN START', clock_timestamp();

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

  -- VC 3.6 NO-CACHE: purge any prior forecast output tables once per run
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
      AND tablename LIKE '%\_instance\_sr\_sq' ESCAPE '\'
    ORDER BY tablename
  LOOP
    t_series_start := clock_timestamp();

    sr_rel  := r.tablename;
    base    := regexp_replace(sr_rel, '_instance_sr_sq$', '');
    sr_qual := format('%I.%I', 'engine', sr_rel);

    RAISE NOTICE '[%] BEGIN series % — scanning latest forecast_id', clock_timestamp(), base;

    EXECUTE format('select forecast_id from %s order by created_at desc limit 1', sr_qual)
      INTO latest_id;
    IF latest_id IS NULL THEN
      RAISE NOTICE '[%] SKIP series % — no forecast_id', clock_timestamp(), base;
      CONTINUE;
    END IF;

    EXECUTE $q$
      select (min(date) + interval '2 years')::date
      from engine.instance_historical
      where forecast_id = $1
    $q$
    USING latest_id
    INTO start_from;
    IF start_from IS NULL THEN
      -- HOTFIX: add base as 2nd arg to match two % placeholders
      RAISE NOTICE '[%] SKIP series % — no historical', clock_timestamp(), base;
      CONTINUE;
    END IF;
    dest_rel  := base || '_instance_forecast_msq';
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
      fv_mae numeric,
      fv_mean_mae numeric,
      fv_mean_mae_c numeric,
      fv_u numeric,
      fv_l numeric,
      mae_comparison text,
      mean_mae_comparison text,
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
    -- Determine destination column names (prefer 'series'/'season'; fallback to legacy)
    dest_series_col := 'series';
    dest_season_col := 'season';
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_series_col;
    IF NOT FOUND THEN dest_series_col := base; END IF;
    PERFORM 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=dest_rel AND column_name=dest_season_col;
    IF NOT FOUND THEN dest_season_col := base || '_yqm'; END IF;


    sr_base_col       := 'sr.' || quote_ident(base || '_q');
    sr_yqm_col        := 'sr.' || quote_ident(base || '_yqm');
    sr_fmsr_a1_col    := 'sr.' || quote_ident(base || '_q_fmsr_a1');
    sr_fmsr_a2_col    := 'sr.' || quote_ident(base || '_q_fmsr_a2');
    sr_fmsr_a2w_col   := 'sr.' || quote_ident(base || '_q_fmsr_a2w');
    sr_fmsr_a3_col    := 'sr.' || quote_ident(base || '_q_fmsr_a3');
    sr_fmsr_a3w_col   := 'sr.' || quote_ident(base || '_q_fmsr_a3w');

    ------------------------------------------------------------------
    -- PASS 1 — CTAS
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 1 — %', t_pass_start, base;

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
        NULL::numeric as fv_mae,
        NULL::numeric as fv_mean_mae,
        NULL::numeric as fv_mean_mae_c,
        NULL::numeric as fv_u,
        NULL::numeric as fv_l,
        NULL::text    as mae_comparison,
        NULL::text    as mean_mae_comparison,
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
      join engine.instance_historical h
        on h.forecast_id = sr.forecast_id
       and h.date        = sr.date
      cross join variants v
      where sr.forecast_id = %L
      order by sr.date, v.column1, v.column3;
    
    -- VC 5.4.3: early trim & error on temp to reduce downstream work
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
    -- keep dest_rel/dest_qual for _instance_forecast_msq
EXECUTE format($i$
      INSERT INTO %1$s (
        forecast_id, "date", value, %2$I, %3$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, fv_error, fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_u, fv_l, mae_comparison, mean_mae_comparison, accuracy_comparison,
        best_fm_count, best_fm_odds, best_fm_sig, fv_interval, fv_interval_c,
        fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, created_at
      )
      SELECT
        forecast_id, "date", value, %4$I, %5$I, model_name, base_model, base_fv,
        fmsr_series, fmsr_value, fv, ABS(value - fv)::numeric AS fv_error, fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_u, fv_l, mae_comparison, mean_mae_comparison, accuracy_comparison,
        best_fm_count, best_fm_odds, best_fm_sig, fv_interval, fv_interval_c,
        fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, now()
      FROM __tmp_forecast_build
    $i$, dest_qual, dest_series_col, dest_season_col, 'series', base || '_yqm');
-- Replace placeholder fv_error with computed fv_error2 (computed at CTAS to avoid a full-table UPDATE)
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    RAISE NOTICE '[%] PASS 1 — % rows: % (%.3f s)', clock_timestamp(), dest_rel, rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);

    IF enable_full_analyze THEN
      EXECUTE format('ANALYZE %s', dest_qual);
    END IF;

    ------------------------------------------------------------------

    -- PASS 3 — fv_mae
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 3 — fv_mae …', t_pass_start;
    EXECUTE format($u$
      WITH m AS (
        SELECT %1$I AS series, model_name, %2$I AS yqm, AVG(fv_error)::numeric AS mae
        FROM %3$s
        WHERE fv_error IS NOT NULL
        GROUP BY %1$I, model_name, %2$I
      )
      UPDATE %3$s t
         SET fv_mae = m.mae
        FROM m
       WHERE t.%1$I       = m.series
         AND t.model_name = m.model_name
         AND t.%2$I       = m.yqm
         AND t.fv_error IS NOT NULL
    $u$,  dest_series_col, dest_season_col, dest_qual);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 3 — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);

    ------------------------------------------------------------------
    -- season_dim temp (for this series)
    ------------------------------------------------------------------
    RAISE NOTICE '[%] BUILD __season_dim …', clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS __season_dim';
    EXECUTE format($u$
      CREATE TEMPORARY TABLE __season_dim AS
      SELECT
        %1$I       AS series,
        model_name AS model_name,
        %2$I       AS yqm,
        MIN(date)  AS season_start,
        MAX(fv_mae) AS season_mae
      FROM %3$s
      WHERE base_fv IS NOT NULL
      GROUP BY %1$I, model_name, %2$I
    $u$,  dest_series_col, dest_season_col, dest_qual);

    
    -- VC 4.6: index temp season_dim
    CREATE INDEX ON __season_dim (series, model_name, yqm);
    ANALYZE __season_dim;
------------------------------------------------------------------
    ------------------------------------------------------------------
    -- PASS 4a — fv_mean_mae & fv_mean_mae_c (strict chronological lag by season_start; exclude current)
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 4a — lagged mean/count …', t_pass_start;
    EXECUTE format($u$
      WITH stats AS (
        SELECT
          s.series,
          s.model_name,
          s.yqm,
          SUM(s.season_mae) OVER (
            PARTITION BY s.series, s.model_name
            ORDER BY s.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_sum,
          COUNT(s.season_mae) OVER (
            PARTITION BY s.series, s.model_name
            ORDER BY s.season_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS prev_cnt
        FROM __season_dim s
      )
      UPDATE %1$s t
         SET fv_mean_mae   = (st.prev_sum / NULLIF(st.prev_cnt,0)),
             fv_mean_mae_c = (st.prev_cnt)::numeric
        FROM stats st
       WHERE t.%2$I       = st.series
         AND t.model_name = st.model_name
         AND t.%3$I       = st.yqm
         AND (
              t.fv_mean_mae   IS DISTINCT FROM (st.prev_sum / NULLIF(st.prev_cnt,0)) OR
              t.fv_mean_mae_c IS DISTINCT FROM (st.prev_cnt)::numeric
         )
    $u$,  dest_qual, dest_series_col, dest_season_col);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 4a — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);

    -- PASS 4b — bands …', t_pass_start;
    EXECUTE format($u$
      UPDATE %1$s
         SET fv_u = fv + fv_mean_mae,
             fv_l = GREATEST(0, fv - fv_mean_mae)
       WHERE fv_mean_mae IS NOT NULL
    $u$,  dest_qual);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 4b — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);

    ------------------------------------------------------------------
    -- A0 cache temp
    ------------------------------------------------------------------
    RAISE NOTICE '[%] BUILD __a0_map …', clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS __a0_map';
    EXECUTE format($u$
      CREATE TEMPORARY TABLE __a0_map AS
      SELECT base_model, %1$I AS yqm,
             MAX(fv_mae)      AS mae0,
             MAX(fv_mean_mae) AS mean_mae0
      FROM %2$s
      WHERE fmsr_series = 'A0'
      GROUP BY base_model, %1$I
    $u$,  dest_season_col, dest_qual);

    
    -- VC 4.6: index temp a0_map
    CREATE INDEX ON __a0_map (base_model, yqm);
    ANALYZE __a0_map;
------------------------------------------------------------------
    -- PASS 5 — intervals / variance / comparisons
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 5 — intervals/variance/comparisons …', t_pass_start;
    EXECUTE format($u$
      UPDATE %2$s t
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
           mae_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mae IS NULL OR a0.mae0 IS NULL THEN NULL
               WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H'
             END,
           mean_mae_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
               WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H'
             END,
           accuracy_comparison =
             CASE
               WHEN t.fmsr_series = 'A0' THEN NULL
               WHEN t.fv_mae IS NULL OR a0.mae0 IS NULL OR t.fv_mean_mae IS NULL OR a0.mean_mae0 IS NULL THEN NULL
               WHEN (CASE WHEN t.fv_mae < a0.mae0 THEN 'L' ELSE 'H' END) = 'L'
                AND (CASE WHEN t.fv_mean_mae < a0.mean_mae0 THEN 'L' ELSE 'H' END) = 'L'
               THEN 'Y' ELSE 'N'
             END
        FROM __a0_map a0
       WHERE t.base_model = a0.base_model
         AND t.%1$I       = a0.yqm
    $u$,  dest_season_col, dest_qual);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 5 — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);

    ------------------------------------------------------------------
------------------------------------------------------------------
    ------------------------------------------------------------------
    -- PASS 6A — counts & variance mean (lag counts by season_start; exclude current)
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 6A — lagged counts/variance mean …', t_pass_start;
    EXECUTE format($u$
      WITH flags AS (
        SELECT
          %1$I AS series,
          model_name,
          %2$I AS yqm,
          MAX(CASE WHEN accuracy_comparison = 'Y' THEN 1 ELSE 0 END) AS acc_y,
          MAX(CASE WHEN fv_interval        = 'Y' THEN 1 ELSE 0 END) AS int_y,
          AVG(CASE WHEN fv_variance IS NOT NULL THEN fv_variance END) AS season_var
        FROM %3$s
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
              t.fv_variance_mean IS DISTINCT FROM round( (st.cum_var_sum / NULLIF(st.cum_var_cnt,0))::numeric, 2 )
         )
         AND (
              t.best_fm_count IS DISTINCT FROM COALESCE(st.prev_best_cnt, 0) OR
              t.fv_interval_c IS DISTINCT FROM COALESCE(st.prev_int_cnt, 0) OR
              t.fv_variance_mean IS NULL
           )
    $u$,  dest_series_col, dest_season_col, dest_qual);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 6A — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);
    ------------------------------------------------------------------
    -- VC 4.7 — local temp p-value lookup to avoid FDW cost
    ------------------------------------------------------------------
    -- Pull only the needed range of n for this build and index it
    ------------------------------------------------------------------
    -- VC 5.4.1 — local temp p-value lookup to avoid FDW overhead
    ------------------------------------------------------------------
    ------------------------------------------------------------------
    -- VC 5.4.3 — local temp p-value lookup to avoid FDW overhead
-- VC 6.0: CHANGE — rename output column names: "<table>" → series, "<table>_yqm" → season; preserve data sources; no logic changes.
    ------------------------------------------------------------------
    BEGIN
      DROP TABLE IF EXISTS binom_p_local;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    EXECUTE format($l$
      CREATE TEMP TABLE binom_p_local AS
      SELECT p.*
      FROM engine.binom_p p
      WHERE p.n <= (SELECT COALESCE(max(fv_mean_mae_c)::int, 0) FROM %s)
    $l$, dest_qual);
    CREATE INDEX ON binom_p_local (n, k);
    ANALYZE binom_p_local;


    -- PASS 6B — odds & significance (extend into forecast rows; require only n>0)
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 6B — odds/significance …', t_pass_start;
    EXECUTE format($u$
      WITH stats AS (
        SELECT
          %1$I AS series,
          model_name,
          %2$I AS yqm,
          COALESCE(fv_mean_mae_c,0)::int AS n,
          COALESCE(best_fm_count,0)::int AS k_best,
          COALESCE(fv_interval_c,0)::int AS k_int
        FROM %3$s
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
        LEFT JOIN binom_p_local p
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
        LEFT JOIN binom_p_local p2
               ON p2.n = i.n AND p2.k = i.k_int
      )
      UPDATE %3$s t
         SET best_fm_odds     = ip.odds_best,
             best_fm_sig = CASE WHEN ip.p_best IS NULL THEN NULL ELSE ip.p_best::numeric END,
             fv_interval_odds = ip.odds_int,
             fv_interval_sig = CASE WHEN ip.p_int IS NULL THEN NULL ELSE ip.p_int::numeric END
        FROM ints_p ip
       WHERE t.%1$I       = ip.series
         AND t.model_name = ip.model_name
         AND t.%2$I       = ip.yqm
    $u$,  dest_series_col, dest_season_col, dest_qual);
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 6B — updated rows: % (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);
    ------------------------------------------------------------------
    -- PASS 7 — final clamp to 4dp on all metric columns
    ------------------------------------------------------------------
    t_pass_start := clock_timestamp();
    RAISE NOTICE '[%] PASS 7 — clamp to 4dp …', t_pass_start;
    EXECUTE format($c$
      UPDATE %s SET
        value            = round(value::numeric, 4),
        base_fv          = round(base_fv::numeric, 4),
        fmsr_value       = round(fmsr_value::numeric, 4),
        fv               = round(fv::numeric, 4),
        fv_error         = CASE WHEN fv_error IS NULL THEN NULL ELSE round(fv_error::numeric, 4) END,
        fv_mae           = CASE WHEN fv_mae IS NULL THEN NULL ELSE round(fv_mae::numeric, 4) END,
        fv_mean_mae      = CASE WHEN fv_mean_mae IS NULL THEN NULL ELSE round(fv_mean_mae::numeric, 4) END,
        fv_u             = CASE WHEN fv_u IS NULL THEN NULL ELSE round(fv_u::numeric, 4) END,
        fv_l             = CASE WHEN fv_l IS NULL THEN NULL ELSE round(fv_l::numeric, 4) END,
        best_fm_odds     = CASE WHEN best_fm_odds IS NULL THEN NULL ELSE round(best_fm_odds::numeric, 4) END,
        best_fm_sig      = CASE WHEN best_fm_sig IS NULL THEN NULL ELSE round(best_fm_sig::numeric, 4) END,fv_interval_odds = CASE WHEN fv_interval_odds IS NULL THEN NULL ELSE round(fv_interval_odds::numeric, 4) END,
        fv_interval_sig  = CASE WHEN fv_interval_sig IS NULL THEN NULL ELSE round(fv_interval_sig::numeric, 4) END,
        fv_variance      = CASE WHEN fv_variance IS NULL THEN NULL ELSE round(fv_variance::numeric, 4) END,
        fv_variance_mean = CASE WHEN fv_variance_mean IS NULL THEN NULL ELSE round(fv_variance_mean::numeric, 4) END
    $c$, dest_qual);
      -- Only rewrite rows where at least one value would change
      EXECUTE format($w$
        UPDATE %s SET
          value            = round(value::numeric, 4),
          base_fv          = round(base_fv::numeric, 4),
          fmsr_value       = round(fmsr_value::numeric, 4),
          fv               = round(fv::numeric, 4),
          fv_error         = CASE WHEN fv_error IS NULL THEN NULL ELSE round(fv_error::numeric, 4) END,
          fv_mae           = CASE WHEN fv_mae IS NULL THEN NULL ELSE round(fv_mae::numeric, 4) END,
          fv_mean_mae      = CASE WHEN fv_mean_mae IS NULL THEN NULL ELSE round(fv_mean_mae::numeric, 4) END,
          fv_u             = CASE WHEN fv_u IS NULL THEN NULL ELSE round(fv_u::numeric, 4) END,
          fv_l             = CASE WHEN fv_l IS NULL THEN NULL ELSE round(fv_l::numeric, 4) END,
          best_fm_odds     = CASE WHEN best_fm_odds IS NULL THEN NULL ELSE round(best_fm_odds::numeric, 4) END,
          best_fm_sig      = CASE WHEN best_fm_sig IS NULL THEN NULL ELSE round(best_fm_sig::numeric, 4) END,
          fv_interval_odds = CASE WHEN fv_interval_odds IS NULL THEN NULL ELSE round(fv_interval_odds::numeric, 4) END,
          fv_interval_sig  = CASE WHEN fv_interval_sig IS NULL THEN NULL ELSE round(fv_interval_sig::numeric, 4) END,
          fv_variance      = CASE WHEN fv_variance IS NULL THEN NULL ELSE round(fv_variance::numeric, 4) END,
          fv_variance_mean = CASE WHEN fv_variance_mean IS NULL THEN NULL ELSE round(fv_variance_mean::numeric, 4) END
        WHERE
             value            IS DISTINCT FROM round(value::numeric, 4)
          OR base_fv          IS DISTINCT FROM round(base_fv::numeric, 4)
          OR fmsr_value       IS DISTINCT FROM round(fmsr_value::numeric, 4)
          OR fv               IS DISTINCT FROM round(fv::numeric, 4)
          OR (fv_error        IS NOT NULL AND fv_error        IS DISTINCT FROM round(fv_error::numeric, 4))
          OR (fv_mae          IS NOT NULL AND fv_mae          IS DISTINCT FROM round(fv_mae::numeric, 4))
          OR (fv_mean_mae     IS NOT NULL AND fv_mean_mae     IS DISTINCT FROM round(fv_mean_mae::numeric, 4))
          OR (fv_u            IS NOT NULL AND fv_u            IS DISTINCT FROM round(fv_u::numeric, 4))
          OR (fv_l            IS NOT NULL AND fv_l            IS DISTINCT FROM round(fv_l::numeric, 4))
          OR (best_fm_odds    IS NOT NULL AND best_fm_odds    IS DISTINCT FROM round(best_fm_odds::numeric, 4))
          OR (best_fm_sig     IS NOT NULL AND best_fm_sig     IS DISTINCT FROM round(best_fm_sig::numeric, 4))
          OR (fv_interval_odds IS NOT NULL AND fv_interval_odds IS DISTINCT FROM round(fv_interval_odds::numeric, 4))
          OR (fv_interval_sig  IS NOT NULL AND fv_interval_sig  IS DISTINCT FROM round(fv_interval_sig::numeric, 4))
          OR (fv_variance     IS NOT NULL AND fv_variance     IS DISTINCT FROM round(fv_variance::numeric, 4))
          OR (fv_variance_mean IS NOT NULL AND fv_variance_mean IS DISTINCT FROM round(fv_variance_mean::numeric, 4))
      $w$, dest_qual);
      GET DIAGNOSTICS rcnt = ROW_COUNT;
    IF enable_full_analyze THEN EXECUTE format('ANALYZE %s', dest_qual); END IF;
    RAISE NOTICE '[%] PASS 7 — clamped % rows (%.3f s)', clock_timestamp(), rcnt,
      EXTRACT(epoch FROM clock_timestamp() - t_pass_start);


    -- Enforce integer types for count columns
    EXECUTE format(
      'ALTER TABLE %s
         ALTER COLUMN best_fm_count    TYPE integer USING COALESCE(best_fm_count,0)::integer,
         ALTER COLUMN fv_interval_c    TYPE integer USING COALESCE(fv_interval_c,0)::integer,
         ALTER COLUMN fv_mean_mae_c    TYPE integer USING COALESCE(fv_mean_mae_c,0)::integer',
      dest_qual
    );
-- Optional physical maintenance (A: gated)
    ------------------------------------------------------------------
    IF enable_cluster_vacuum THEN
      t_pass_start := clock_timestamp();
      RAISE NOTICE '[%] CLUSTER/VACUUM …', t_pass_start;
      BEGIN
        EXECUTE format('ALTER TABLE %s CLUSTER ON %I', dest_qual, idx_date_name);
        EXECUTE format('CLUSTER %s USING %I', dest_qual, idx_date_name);
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'CLUSTER skipped: %', SQLERRM;
      END;
      BEGIN
        EXECUTE format('VACUUM (FREEZE, ANALYZE) %s', dest_qual);
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'VACUUM skipped: %', SQLERRM;
      END;
      RAISE NOTICE '[%] CLUSTER/VACUUM done (%.3f s)', clock_timestamp(),
        EXTRACT(epoch FROM clock_timestamp() - t_pass_start);
    END IF;

    RAISE NOTICE '[%] COMPLETE % (series elapsed: %.3f s)', clock_timestamp(), dest_rel,
      EXTRACT(epoch FROM clock_timestamp() - t_series_start);
  END LOOP;

  RAISE NOTICE '[%] ALL DONE (total elapsed: %.3f s)', clock_timestamp(),
    EXTRACT(epoch FROM clock_timestamp() - t_run_start);
END

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION engine.build_forecast_msq()
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  PERFORM engine.build_forecast_msq_core();
END;
$$;

GRANT EXECUTE ON FUNCTION engine.build_forecast_msq() TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.build_forecast_msq_core() TO matrix_reader, tsf_engine_app;
