-- V14_11_TSF_Full_Views_Classical.sql
-- Generated: 2025-10-04
-- Purpose: Rebuild FULL-stage per-table/per-model views and final view with new CANNON-era columns.
-- Notes:
--   * Output columns updated: expose fv_mape/mae/rmse (+ means/counts) and ci85/ci90/ci95 lows/highs.
--   * Removed everywhere: fv_variance, fv_variance_mean, fv_interval_odds, fv_interval_sig, low, high.
--   * Selection logic to choose best forecast per (forecast_id, date) keeps the same spirit:
--       ORDER BY fv_mean_mape ASC NULLS LAST, fv_mean_mape_c DESC NULLS LAST, _src_ord ASC.
--   * ARIMA_M / SES_M / HWES_M appended from engine.instance_historical as before.

BEGIN;
SET search_path = engine, public;

CREATE TABLE IF NOT EXISTS engine.prebaked_views(view_name text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS engine.view_cache_refresh_log(
    view_name text,
    forecast_id uuid,
    refreshed_at timestamptz default now()
);

-- Discover source forecast tables for MS / MSQ / MSQM
CREATE TEMP TABLE pg_temp._tsf_src_tables AS
SELECT
    'engine'::text AS schema_name,
    c.relname      AS table_name,
    CASE
        WHEN c.relname ~ '_instance_forecast_ms$'   THEN 'MS'
        WHEN c.relname ~ '_instance_forecast_msq$'  THEN 'MSQ'
        WHEN c.relname ~ '_instance_forecast_msqm$' THEN 'MSQM'
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

-- Layer 1: Per-table FULL stage views (fv_mean_mape_c >= 5)
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
            CREATE OR REPLACE VIEW engine.%I AS
            SELECT
                forecast_id,
                date,
                value,
                model_name,
                fv,
                fv_mape, fv_mean_mape, fv_mean_mape_c,
                fv_mae,  fv_mean_mae,  fv_mean_mae_c,
                fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
                ci85_low, ci85_high,
                ci90_low, ci90_high,
                ci95_low, ci95_high
            FROM %I.%I
            WHERE fv IS NOT NULL
              AND fv <> 0
              AND (fv_mean_mape_c IS NULL OR fv_mean_mape_c >= 5)
        $v$, v_view_name, rec.schema_name, rec.table_name);

        EXECUTE v_sql;
        INSERT INTO engine.prebaked_views(view_name) VALUES (v_view_name)
        ON CONFLICT (view_name) DO NOTHING;
    END LOOP;
END $do$;

-- Layer 2: Per-model FULL stage views: Union per-series views with a stable series preference
DO $do2$
DECLARE
    m RECORD;
    s RECORD;
    v_view_name text;
    v_union_sql text;
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
            v_ord := CASE s.series WHEN 'MS' THEN 1 WHEN 'MSQ' THEN 2 ELSE 3 END;
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
                    u.fv,
                    u.fv_mape, u.fv_mean_mape, u.fv_mean_mape_c,
                    u.fv_mae,  u.fv_mean_mae,  u.fv_mean_mae_c,
                    u.fv_rmse, u.fv_mean_rmse, u.fv_mean_rmse_c,
                    u.ci85_low, u.ci85_high,
                    u.ci90_low, u.ci90_high,
                    u.ci95_low, u.ci95_high
                FROM (
                    %s
                ) AS u
            $v$, v_view_name, v_union_sql);

            EXECUTE v_sql;
            INSERT INTO engine.prebaked_views(view_name) VALUES (v_view_name)
            ON CONFLICT (view_name) DO NOTHING;
        END IF;
    END LOOP;
END $do2$;

-- Layer 3: Cache builder for final view (keep selection ORDER BY consistent)
CREATE TEMP TABLE pg_temp._vw_full_union AS
SELECT
    v.view_name,
    format('SELECT * FROM %I.%I', 'engine', v.view_name) AS sel
FROM engine.prebaked_views v
WHERE v.view_name ~ '_vw_full_stage$'
ORDER BY v.view_name;

-- Build UNION ALL SQL across all model-stage views
DO $do3$
DECLARE
    r RECORD;
    sql_union text := NULL;
BEGIN
    FOR r IN SELECT * FROM pg_temp._vw_full_union LOOP
        IF sql_union IS NULL THEN
            sql_union := r.sel;
        ELSE
            sql_union := sql_union || ' UNION ALL ' || r.sel;
        END IF;
    END LOOP;

    -- Cache table to drive final selection
    CREATE TABLE IF NOT EXISTS engine.tsf_vw_full_cache AS
    SELECT NULL::uuid    AS forecast_id,
           NULL::text    AS forecast_name,
           NULL::date    AS date,
           NULL::numeric AS value,
           NULL::text    AS model_name,
           NULL::numeric AS fv,
           NULL::numeric AS fv_mape, NULL::numeric AS fv_mean_mape, NULL::numeric AS fv_mean_mape_c,
           NULL::numeric AS fv_mae,  NULL::numeric AS fv_mean_mae,  NULL::numeric AS fv_mean_mae_c,
           NULL::numeric AS fv_rmse, NULL::numeric AS fv_mean_rmse, NULL::numeric AS fv_mean_rmse_c,
           NULL::numeric AS ci85_low, NULL::numeric AS ci85_high,
           NULL::numeric AS ci90_low, NULL::numeric AS ci90_high,
           NULL::numeric AS ci95_low, NULL::numeric AS ci95_high
    WITH NO DATA;

    -- Fill the final view from a subquery Y that tags a row-number per (forecast_id,date)
    -- using the original ordering sans binomial tie-breakers.
    EXECUTE format($F$
        CREATE OR REPLACE VIEW engine.tsf_vw_full AS
        SELECT
            c.forecast_name,
            c.date,
            c.value,
            c.model_name,
            c.fv,
            c.fv_mape, c.fv_mean_mape, c.fv_mean_mape_c,
            c.fv_mae,  c.fv_mean_mae,  c.fv_mean_mae_c,
            c.fv_rmse, c.fv_mean_rmse, c.fv_mean_rmse_c,
            c.ci85_low, c.ci85_high,
            c.ci90_low, c.ci90_high,
            c.ci95_low, c.ci95_high,
            ih.arima_m AS ARIMA_M,
            ih.ses_m   AS SES_M,
            ih.hwes_m  AS HWES_M
        FROM (
            SELECT
                g.forecast_id,
                COALESCE(fr.forecast_name, g.forecast_id::text) AS forecast_name,
                g.date,
                g.value,
                g.model_name,
                g.fv,
                g.fv_mape, g.fv_mean_mape, g.fv_mean_mape_c,
                g.fv_mae,  g.fv_mean_mae,  g.fv_mean_mae_c,
                g.fv_rmse, g.fv_mean_rmse, g.fv_mean_rmse_c,
                g.ci85_low, g.ci85_high,
                g.ci90_low, g.ci90_high,
                g.ci95_low, g.ci95_high
            FROM (
                SELECT y.*, row_number() OVER (
                    PARTITION BY y.forecast_id, y.date
                    ORDER BY y.fv_mean_mape ASC NULLS LAST,
                             y.fv_mean_mape_c DESC NULLS LAST,
                             y._src_ord ASC
                ) AS _rn
                FROM ( %s ) AS y
                WHERE y.fv IS NOT NULL AND y.fv <> 0 AND (y.fv_mean_mape_c IS NULL OR y.fv_mean_mape_c >= 5)
            ) AS g
            LEFT JOIN engine.forecast_registry fr
              ON fr.forecast_id = g.forecast_id
            WHERE g._rn = 1
        ) AS c
        LEFT JOIN engine.instance_historical ih
          ON ih.forecast_id = c.forecast_id AND ih.date = c.date
    $F$, sql_union);
END $do3$;

COMMIT;
