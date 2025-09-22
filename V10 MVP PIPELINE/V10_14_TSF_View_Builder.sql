-- ===================================================================
-- File: V10_14_TSF_View_Builder.sql
-- Purpose: Discover forecast source tables and (re)create TSF views:
--          1) Per-table daily winner views
--          2) Per-model daily winner views (S/SQ/SQM aggregation)
--          3) Global TSF daily winner view across all models
-- Author: ChatGPT (per user instruction: SQL only, downloadable file)
-- Date: 2025-09-22
-- Changes vs V10_14:
--   * Use distinct dollar-quote tags ($v$) inside PL/pgSQL to avoid parser
--     confusion that can surface as 'syntax error at or near "CREATE"'.
--   * Add explicit LANGUAGE plpgsql on DO blocks.
--   * Use CREATE OR REPLACE VIEW for idempotency.
--   * Quote identifiers robustly when referencing derived view names.
-- ===================================================================

BEGIN;

SET search_path = engine, public;

-- Helper: drop a view if it exists (safe no-op when missing).
CREATE OR REPLACE FUNCTION engine._drop_view_if_exists(v_schema text, v_view text)
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_views
        WHERE schemaname = v_schema
          AND viewname  = v_view
    ) THEN
        EXECUTE format('DROP VIEW %I.%I', v_schema, v_view);
    END IF;
END;
$fn$;

-- ===================================================================
-- Phase 1: Discover source tables in schema 'engine'
-- We look for tables named like: <model_key>_instance_forecast_ms / msq / msqm
-- ===================================================================

DROP TABLE IF EXISTS pg_temp._tsf_src_tables;
CREATE TEMP TABLE pg_temp._tsf_src_tables AS
SELECT
    n.nspname        AS schema_name,
    c.relname        AS table_name,
    CASE
        WHEN c.relname ~ '_instance_forecast_msqm$' THEN 'SQM'
        WHEN c.relname ~ '_instance_forecast_msq$'  THEN 'SQ'
        WHEN c.relname ~ '_instance_forecast_ms$'   THEN 'S'
        ELSE NULL
    END             AS series,
    CASE
        WHEN c.relname ~ '_instance_forecast_msqm$' THEN regexp_replace(c.relname, '_instance_forecast_msqm$', '')
        WHEN c.relname ~ '_instance_forecast_msq$'  THEN regexp_replace(c.relname, '_instance_forecast_msq$',  '')
        WHEN c.relname ~ '_instance_forecast_ms$'   THEN regexp_replace(c.relname, '_instance_forecast_ms$',   '')
        ELSE NULL
    END             AS model_key
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'engine'
  AND c.relkind = 'r'
  AND (
       c.relname ~ '_instance_forecast_ms$'
    OR c.relname ~ '_instance_forecast_msq$'
    OR c.relname ~ '_instance_forecast_msqm$'
  )
;

-- ===================================================================
-- Phase 2: Per-table "vw_daily_best" views
-- ===================================================================

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
        v_view_name := rec.table_name || '_vw_daily_best';
        PERFORM engine._drop_view_if_exists(rec.schema_name, v_view_name);

        v_sql := format($v$
            CREATE OR REPLACE VIEW %I.%I AS
            SELECT
                forecast_id,
                date,
                value,
                fv_l,
                fv,
                fv_u,
                fv_mean_mae,
                fv_interval_odds,
                fv_interval_sig,
                fv_variance_mean,
                fv_mean_mae_c
            FROM (
                SELECT
                    t.*,
                    row_number() OVER (
                        PARTITION BY t.forecast_id, t.date
                        ORDER BY
                            t.fv_mean_mae ASC NULLS LAST,
                            t.fv_interval_odds DESC NULLS LAST,
                            t.fv_interval_sig ASC NULLS LAST,
                            t.fv_mean_mae_c DESC NULLS LAST,
                            t.ctid ASC
                    ) AS _rn
                FROM %I.%I t
                WHERE t.fv_mean_mae IS NOT NULL
            ) z
            WHERE z._rn = 1;
        $v$, rec.schema_name, v_view_name, rec.schema_name, rec.table_name);

        EXECUTE v_sql;
    END LOOP;
END
$do$ LANGUAGE plpgsql;

-- ===================================================================
-- Phase 3: Per-model "vw_daily_best" views (combine S/SQ/SQM)
-- ===================================================================

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
        v_view_name := m.model_key || '_vw_daily_best';
        PERFORM engine._drop_view_if_exists('engine', v_view_name);

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
                                      v_ord, 'engine', s.table_name || '_vw_daily_best');
            ELSE
                v_union_sql := v_union_sql || format(' UNION ALL SELECT * , %s AS _src_ord FROM %I.%I',
                                                     v_ord, 'engine', s.table_name || '_vw_daily_best');
            END IF;
        END LOOP;

        IF v_union_sql IS NOT NULL THEN
            v_sql := format($v$
                CREATE OR REPLACE VIEW engine.%I AS
                SELECT
                    forecast_id,
                    date,
                    value,
                    fv_l,
                    fv,
                    fv_u,
                    fv_mean_mae,
                    fv_interval_odds,
                    fv_interval_sig,
                    fv_variance_mean,
                    fv_mean_mae_c
                FROM (
                    SELECT
                        u.*,
                        row_number() OVER (
                            PARTITION BY u.forecast_id, u.date
                            ORDER BY
                                u.fv_mean_mae ASC NULLS LAST,
                                u.fv_interval_odds DESC NULLS LAST,
                                u.fv_interval_sig ASC NULLS LAST,
                                u.fv_mean_mae_c DESC NULLS LAST,
                                u._src_ord ASC
                        ) AS _rn
                    FROM ( %s ) AS u
                ) s
                WHERE s._rn = 1;
            $v$, v_view_name, v_union_sql);

            EXECUTE v_sql;
        END IF;
    END LOOP;
END
$do$ LANGUAGE plpgsql;

-- ===================================================================
-- Phase 4: Global view engine.tsf_vw_daily_best across all models
-- ===================================================================

DO $do$
DECLARE
    m RECORD;
    v_union_sql text := NULL;
    v_sql text;
BEGIN
    -- Collect only per-model views (exclude per-table views)
    FOR m IN
        SELECT viewname
        FROM pg_views
        WHERE schemaname = 'engine'
          AND viewname ~ '_vw_daily_best$'
          AND viewname NOT LIKE '%_instance_forecast_%'
        ORDER BY viewname
    LOOP
        IF v_union_sql IS NULL THEN
            v_union_sql := format('SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', m.viewname);
        ELSE
            v_union_sql := v_union_sql || format(' UNION ALL SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', m.viewname);
        END IF;
    END LOOP;

    PERFORM engine._drop_view_if_exists('engine', 'tsf_vw_daily_best');

    IF v_union_sql IS NOT NULL THEN
        v_sql := format($v$
            CREATE OR REPLACE VIEW engine.tsf_vw_daily_best AS
            SELECT
                forecast_id,
                date,
                value,
                fv_l,
                fv,
                fv_u,
                fv_mean_mae,
                fv_interval_odds,
                fv_interval_sig,
                fv_variance_mean,
                fv_mean_mae_c
            FROM (
                SELECT
                    g.*,
                    row_number() OVER (
                        PARTITION BY g.forecast_id, g.date
                        ORDER BY
                            g.fv_mean_mae ASC NULLS LAST,
                            g.fv_interval_odds DESC NULLS LAST,
                            g.fv_interval_sig ASC NULLS LAST,
                            g.fv_mean_mae_c DESC NULLS LAST,
                            g._src_ord ASC
                    ) AS _rn
                FROM ( %s ) AS g
            ) x
            WHERE x._rn = 1;
        $v$, v_union_sql);

        EXECUTE v_sql;
    END IF;
END
$do$ LANGUAGE plpgsql;

-- Optional cleanup
DROP FUNCTION IF EXISTS engine._drop_view_if_exists(text, text);

COMMIT;

-- ============================ END OF FILE ============================
