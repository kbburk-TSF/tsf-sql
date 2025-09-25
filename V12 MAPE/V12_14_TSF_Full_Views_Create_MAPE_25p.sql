-- ===================================================================
-- File: V12_14_TSF_Full_Views_Create_MAPE_25p.sql
-- Updated: 2025-09-24
-- CHANGE: MAE→MAPE column cascade; rank by lowest fv_mean_mape;
--         enforce fv_mean_mape_c >= 5 at all levels.
-- ===================================================================


-- ===================================================================
-- File: V11_14_TSF_Full_Views_Create_fix.sql
-- Updated: 2025-09-24 19:06
-- CHANGE: Enforce fv_mean_mape_c >= 5 before ranking/selection at ALL levels.
-- ===================================================================

BEGIN;
SET search_path = engine, public;

CREATE TABLE IF NOT EXISTS engine.prebaked_views(view_name text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS engine.view_cache_refresh_log(
    view_name text,
    forecast_id uuid,
    started_at timestamptz,
    finished_at timestamptz,
    rows_written bigint,
    ok boolean,
    error_text text
);

DROP TABLE IF EXISTS pg_temp._tsf_src_tables;
CREATE TEMP TABLE pg_temp._tsf_src_tables AS
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE
        WHEN c.relname ~ '_instance_forecast_msqm$' THEN 'SQM'
        WHEN c.relname ~ '_instance_forecast_msq$'  THEN 'SQ'
        WHEN c.relname ~ '_instance_forecast_ms$'   THEN 'S'
        ELSE NULL
    END AS series,
    CASE
        WHEN c.relname ~ '_instance_forecast_msqm$' THEN regexp_replace(c.relname, '_instance_forecast_msqm$', '')
        WHEN c.relname ~ '_instance_forecast_msq$'  THEN regexp_replace(c.relname, '_instance_forecast_msq$',  '')
        WHEN c.relname ~ '_instance_forecast_ms$'   THEN regexp_replace(c.relname, '_instance_forecast_ms$',   '')
        ELSE NULL
    END AS model_key
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'engine'
  AND c.relkind  = 'r'
  AND (
       c.relname ~ '_instance_forecast_ms$'
    OR c.relname ~ '_instance_forecast_msq$'
    OR c.relname ~ '_instance_forecast_msqm$'
  )
;

-- Layer 1: Per-table FULL stage views (ADD: fv_mean_mape_c >= 5 filter)
DO $do$
DECLARE
    rec RECORD;
    v_view_name text;
    v_sql text;
BEGIN
    FOR rec IN
        SELECT schema_name, table_name
        FROM pg_temp._tsf_src_tables
        ORDER BY table_name
    LOOP
        v_view_name := rec.table_name || '_vw_full_stage';

        v_sql := format($v$
            CREATE OR REPLACE VIEW %I.%I AS
            SELECT
                t_w.forecast_id,
                t_w.date,
                t_w.value,
                t_w.model_name,
                t_w.fv_l,
                t_w.fv,
                t_w.fv_u,
                t_w.fv_mean_mape,
                t_w.fv_mean_mape_c,
                t_w.fv_interval_odds,
                t_w.fv_interval_sig,
                t_w.fv_variance,
                t_w.fv_variance_mean
            FROM (
                SELECT
                    t.*,
                    row_number() OVER (
                        PARTITION BY t.forecast_id, t.date
                        ORDER BY
                            t.fv_mean_mape ASC NULLS LAST,
                            t.fv_interval_odds DESC NULLS LAST,
                            t.fv_interval_sig ASC NULLS LAST,
                            t.fv_mean_mape_c DESC NULLS LAST,
                            t.ctid ASC
                    ) AS _rn
                FROM %I.%I t
                WHERE t.fv_mean_mape IS NOT NULL
                  AND t.fv_mean_mape_c >= 5
            ) AS t_w
            WHERE t_w._rn = 1;
        $v$, rec.schema_name, v_view_name, rec.schema_name, rec.table_name);

        EXECUTE v_sql;
    END LOOP;
END
$do$ LANGUAGE plpgsql;

-- Layer 2: Per-model FULL stage views
DO $do$
DECLARE
    m RECORD;
    s RECORD;
    v_union_sql text;
    v_view_name text;
    v_sql text;
    v_ord int;
BEGIN
    FOR m IN
        SELECT DISTINCT model_key
        FROM pg_temp._tsf_src_tables
        WHERE model_key IS NOT NULL
        ORDER BY model_key
    LOOP
        v_view_name := m.model_key || '_vw_full_stage';
        v_union_sql := NULL;

        FOR s IN
            SELECT table_name, series
            FROM pg_temp._tsf_src_tables
            WHERE model_key = m.model_key
            ORDER BY series
        LOOP
            v_ord := CASE s.series WHEN 'S' THEN 1 WHEN 'SQ' THEN 2 ELSE 3 END;
            IF v_union_sql IS NULL THEN
                v_union_sql := format('SELECT * , %s AS _src_ord FROM %I.%I',
                                      v_ord, 'engine', s.table_name || '_vw_full_stage');
            ELSE
                v_union_sql := v_union_sql || format(' UNION ALL SELECT * , %s AS _src_ord FROM %I.%I',
                                                     v_ord, 'engine', s.table_name || '_vw_full_stage');
            END IF;
        END LOOP;

        IF v_union_sql IS NOT NULL THEN
            v_sql := format($v$
                CREATE OR REPLACE VIEW engine.%I AS
                SELECT
                    u.forecast_id,
                    u.date,
                    u.value,
                    u.model_name,
                    u.fv_l,
                    u.fv,
                    u.fv_u,
                    u.fv_mean_mape,
                    u.fv_mean_mape_c,
                    u.fv_interval_odds,
                    u.fv_interval_sig,
                    u.fv_variance,
                    u.fv_variance_mean
                FROM (
                    SELECT
                        x.*,
                        row_number() OVER (
                            PARTITION BY x.forecast_id, x.date
                            ORDER BY
                                x.fv_mean_mape ASC NULLS LAST,
                                x.fv_interval_odds DESC NULLS LAST,
                                x.fv_interval_sig ASC NULLS LAST,
                                x.fv_mean_mape_c DESC NULLS LAST,
                                x._src_ord ASC
                        ) AS _rn
                    FROM ( %s ) AS x
                ) u
                WHERE u._rn = 1;
            $v$, v_view_name, v_union_sql);

            EXECUTE v_sql;
        END IF;
    END LOOP;
END
$do$ LANGUAGE plpgsql;

-- Cache table + backfill for tsf_vw_full
DROP TABLE IF EXISTS engine.tsf_vw_full_cache CASCADE;
CREATE TABLE engine.tsf_vw_full_cache(
    forecast_id uuid NOT NULL,
    forecast_name text NOT NULL,
    date date NOT NULL,
    value numeric,
    model_name text,
    fv_l numeric,
    fv numeric,
    fv_u numeric,
    fv_mean_mape numeric,
    fv_mean_mape_c numeric,
    fv_interval_odds text,
    fv_interval_sig numeric,
    fv_variance text,
    fv_variance_mean text,
    low numeric,
    high numeric,
    CONSTRAINT tsf_vw_full_cache_uk UNIQUE (forecast_id, date)
);
CREATE INDEX tsf_vw_full_cache_ix_fid_date ON engine.tsf_vw_full_cache(forecast_id, date);

DO $do$
DECLARE
    m RECORD;
    v_union_sql text := NULL;
    v_ins_sql text;
BEGIN
    FOR m IN
        SELECT viewname
        FROM pg_views
        WHERE schemaname = 'engine'
          AND viewname ~ '_vw_full_stage$'
        ORDER BY viewname
    LOOP
        IF v_union_sql IS NULL THEN
            v_union_sql := format('SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', m.viewname);
        ELSE
            v_union_sql := v_union_sql || format(' UNION ALL SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', m.viewname);
        END IF;
    END LOOP;

    IF v_union_sql IS NULL THEN
        RETURN;
    END IF;

    EXECUTE 'TRUNCATE TABLE engine.tsf_vw_full_cache';

    v_ins_sql := format($SQL$
        INSERT INTO engine.tsf_vw_full_cache
        (forecast_id, forecast_name, date, value, model_name, fv_l, fv, fv_u,
         fv_mean_mape, fv_mean_mape_c, fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, low, high)
        SELECT
            g.forecast_id,
            COALESCE(fr.forecast_name, g.forecast_id::text) AS forecast_name,
            g.date,
            g.value,
            g.model_name,
            g.fv_l, g.fv, g.fv_u,
            g.fv_mean_mape,
            g.fv_mean_mape_c,
            to_char(g.fv_interval_odds * 100.0, 'FM999990.###') || '%%' AS fv_interval_odds,
            g.fv_interval_sig,
            to_char(g.fv_variance * 100.0, 'FM999990.###') || '%%' AS fv_variance,
            to_char(g.fv_variance_mean * 100.0, 'FM999990.###') || '%%' AS fv_variance_mean,
            CASE WHEN g.fv_variance_mean IS NULL THEN g.fv_l
                 ELSE g.fv_l - (g.fv_l * g.fv_variance_mean) END AS low,
            CASE WHEN g.fv_variance_mean IS NULL THEN g.fv_u
                 ELSE g.fv_u + (g.fv_u * g.fv_variance_mean) END AS high
        FROM (
            SELECT
                y.*,
                row_number() OVER (
                    PARTITION BY y.forecast_id, y.date
                    ORDER BY
                        y.fv_mean_mape ASC NULLS LAST,
                        y.fv_interval_odds DESC NULLS LAST,
                        y.fv_interval_sig ASC NULLS LAST,
                        y.fv_mean_mape_c DESC NULLS LAST,
                        y._src_ord ASC
                ) AS _rn
            FROM ( %s ) AS y
        ) g
        LEFT JOIN engine.forecast_registry fr
          ON fr.forecast_id::text = g.forecast_id::text
        WHERE g._rn = 1
    $SQL$, v_union_sql);
    EXECUTE v_ins_sql;

    EXECUTE 'ANALYZE engine.tsf_vw_full_cache';
END
$do$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW engine.tsf_vw_full AS
SELECT
    forecast_name,
    date,
    value,
    model_name,
    fv_l,
    fv,
    fv_u,
    fv_mean_mape,
    fv_mean_mape_c,
    fv_interval_odds,
    fv_interval_sig,
    fv_variance,
    fv_variance_mean,
    low,
    high
FROM engine.tsf_vw_full_cache;

INSERT INTO engine.prebaked_views(view_name) VALUES
  ('engine.tsf_vw_full'),
  ('engine.tsf_vw_daily_best_arima_a0'),
  ('engine.tsf_vw_daily_best_ses_a0'),
  ('engine.tsf_vw_daily_best_hwes_a0')
ON CONFLICT (view_name) DO NOTHING;

-- Refresh API (unchanged)
CREATE OR REPLACE FUNCTION engine._refresh_tsf_vw_full_slice(p_forecast_id uuid)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v_union_sql text := NULL; v_sql text; v_rows bigint := 0; r record;
BEGIN
  FOR r IN SELECT viewname FROM pg_views WHERE schemaname='engine' AND viewname ~ '_vw_full_stage$' ORDER BY viewname LOOP
    IF v_union_sql IS NULL THEN
      v_union_sql := format('SELECT * , 1 AS _src_ord FROM %I.%I','engine',r.viewname);
    ELSE
      v_union_sql := v_union_sql || format(' UNION ALL SELECT * , 1 AS _src_ord FROM %I.%I','engine',r.viewname);
    END IF;
  END LOOP;
  IF v_union_sql IS NULL THEN RETURN 0; END IF;
  DELETE FROM engine.tsf_vw_full_cache WHERE forecast_id = p_forecast_id;
  v_sql := format($SQL$
    INSERT INTO engine.tsf_vw_full_cache
      (forecast_id, forecast_name, date, value, model_name, fv_l, fv, fv_u,
       fv_mean_mape, fv_mean_mape_c, fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, low, high)
    SELECT
        g.forecast_id,
        COALESCE(fr.forecast_name, g.forecast_id::text) AS forecast_name,
        g.date, g.value, g.model_name,
        g.fv_l, g.fv, g.fv_u,
        g.fv_mean_mape, g.fv_mean_mape_c,
        to_char(g.fv_interval_odds * 100.0, 'FM999990.###') || '%%',
        g.fv_interval_sig,
        to_char(g.fv_variance * 100.0, 'FM999990.###') || '%%',
        to_char(g.fv_variance_mean * 100.0, 'FM999990.###') || '%%',
        CASE WHEN g.fv_variance_mean IS NULL THEN g.fv_l ELSE g.fv_l - (g.fv_l * g.fv_variance_mean) END,
        CASE WHEN g.fv_variance_mean IS NULL THEN g.fv_u ELSE g.fv_u + (g.fv_u * g.fv_variance_mean) END
    FROM (
        SELECT y.*, row_number() OVER (
            PARTITION BY y.forecast_id, y.date
            ORDER BY y.fv_mean_mape ASC NULLS LAST, y.fv_interval_odds DESC NULLS LAST,
                     y.fv_interval_sig ASC NULLS LAST, y.fv_mean_mape_c DESC NULLS LAST, y._src_ord ASC
        ) AS _rn
        FROM ( %s ) AS y
    ) g
    LEFT JOIN engine.forecast_registry fr ON fr.forecast_id::text = g.forecast_id::text
    WHERE g._rn = 1 AND g.forecast_id = $1
  $SQL$, v_union_sql);
  EXECUTE v_sql USING p_forecast_id;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  EXECUTE 'ANALYZE engine.tsf_vw_full_cache';
  RETURN v_rows;
END $$;

CREATE OR REPLACE FUNCTION engine._rebuild_daily_winner_cache(p_view_name text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v_cache regclass; v_src regclass; v_rows bigint := 0; v_sql text;
BEGIN
  IF p_view_name = 'engine.tsf_vw_daily_best_arima_a0' THEN
    v_cache := 'engine.tsf_vw_daily_best_arima_a0_cache'::regclass;
    v_src   := 'engine.tsf_vw_daily_best_arima_a0_src'::regclass;
  ELSIF p_view_name = 'engine.tsf_vw_daily_best_ses_a0' THEN
    v_cache := 'engine.tsf_vw_daily_best_ses_a0_cache'::regclass;
    v_src   := 'engine.tsf_vw_daily_best_ses_a0_src'::regclass;
  ELSIF p_view_name = 'engine.tsf_vw_daily_best_hwes_a0' THEN
    v_cache := 'engine.tsf_vw_daily_best_hwes_a0_cache'::regclass;
    v_src   := 'engine.tsf_vw_daily_best_hwes_a0_src'::regclass;
  ELSE
    RETURN 0;
  END IF;
  EXECUTE 'TRUNCATE TABLE '||v_cache::text;
  v_sql := 'INSERT INTO '||v_cache::text||' SELECT * FROM '||v_src::text;
  EXECUTE v_sql; GET DIAGNOSTICS v_rows = ROW_COUNT;
  EXECUTE 'ANALYZE '||v_cache::text;
  RETURN v_rows;
END $$;

CREATE OR REPLACE FUNCTION engine.refresh_prebaked_slice(view_name text, forecast_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE ok_lock boolean; t0 timestamptz := clock_timestamp(); t1 timestamptz; v_rows bigint := 0; v_err text := NULL;
BEGIN
  ok_lock := pg_try_advisory_xact_lock(hashtext(view_name), hashtext(COALESCE(forecast_id::text,'')));
  IF NOT ok_lock THEN
    INSERT INTO engine.view_cache_refresh_log(view_name, forecast_id, started_at, finished_at, rows_written, ok, error_text)
    VALUES (view_name, forecast_id, t0, clock_timestamp(), 0, true, 'skipped: lock held');
    RETURN;
  END IF;
  BEGIN
    IF view_name = 'engine.tsf_vw_full' THEN
      v_rows := engine._refresh_tsf_vw_full_slice(forecast_id);
    ELSIF view_name IN ('engine.tsf_vw_daily_best_arima_a0','engine.tsf_vw_daily_best_ses_a0','engine.tsf_vw_daily_best_hwes_a0') THEN
      v_rows := engine._rebuild_daily_winner_cache(view_name);
    ELSE
      INSERT INTO engine.view_cache_refresh_log(view_name, forecast_id, started_at, finished_at, rows_written, ok, error_text)
      VALUES (view_name, forecast_id, t0, clock_timestamp(), 0, true, 'skipped: view not supported yet');
      RETURN;
    END IF;
  EXCEPTION WHEN OTHERS THEN v_err := SQLERRM; END;
  t1 := clock_timestamp();
  IF v_err IS NULL THEN
    INSERT INTO engine.view_cache_refresh_log(view_name, forecast_id, started_at, finished_at, rows_written, ok, error_text)
    VALUES (view_name, forecast_id, t0, t1, COALESCE(v_rows,0), true, NULL);
  ELSE
    INSERT INTO engine.view_cache_refresh_log(view_name, forecast_id, started_at, finished_at, rows_written, ok, error_text)
    VALUES (view_name, forecast_id, t0, t1, 0, false, v_err);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION engine.refresh_all_prebaked_views(forecast_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT view_name FROM engine.prebaked_views ORDER BY view_name LOOP
    PERFORM engine.refresh_prebaked_slice(r.view_name, forecast_id);
  END LOOP;
END $$;

GRANT EXECUTE ON FUNCTION engine.refresh_prebaked_slice(text, uuid) TO aq_engine_owner, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.refresh_all_prebaked_views(uuid) TO aq_engine_owner, tsf_engine_app;

COMMIT;
