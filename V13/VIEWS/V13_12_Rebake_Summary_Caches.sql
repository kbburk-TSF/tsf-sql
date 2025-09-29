-- V13_12_Rebake_Summary_Caches.sql
-- 2025-09-27: FIX — Rebuild cache directly from the existing *_vw_full_stage views (no info_schema aggregation).
--              Mirrors the proven union-and-rank pattern used in V13_11_TSF_Full_Views_Classical.sql.
--              No math/logic changes; only repack pipeline to avoid 42601/42803 issues.

SET search_path = engine, public;

CREATE OR REPLACE FUNCTION engine.rebake_summary_caches()
RETURNS TABLE(cache_name text, rows_written bigint)
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  v_union_sql text := NULL;
  v_ins_sql   text;
  v_rows      bigint := 0;
  r           record;
BEGIN
  -- Assemble UNION ALL from all per-table stage views already created by V13_11:
  FOR r IN
    SELECT viewname
    FROM pg_views
    WHERE schemaname = 'engine'
      AND viewname ~ '_vw_full_stage$'
    ORDER BY viewname
  LOOP
    IF v_union_sql IS NULL THEN
      v_union_sql := format('SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', r.viewname);
    ELSE
      v_union_sql := v_union_sql || format(' UNION ALL SELECT * , 1 AS _src_ord FROM %I.%I', 'engine', r.viewname);
    END IF;
  END LOOP;

  IF v_union_sql IS NULL THEN
    RETURN QUERY SELECT 'engine.tsf_vw_full_cache'::text, 0::bigint;
    RETURN;
  END IF;

  EXECUTE 'TRUNCATE TABLE engine.tsf_vw_full_cache';

  -- Exact column list matches table definition; SELECT list mirrors V13_11 logic.
  v_ins_sql := format($SQL$
    INSERT INTO engine.tsf_vw_full_cache
      (forecast_id, forecast_name, date, value, model_name, fv_l, fv, fv_u,
       fv_mean_mape, fv_mean_mape_c, fv_interval_odds, fv_interval_sig,
       fv_variance, fv_variance_mean, low, high)
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
      WHERE y.fv IS NOT NULL AND y.fv <> 0 AND y.fv_mean_mape_c >= 5
    ) g
    LEFT JOIN engine.forecast_registry fr
      ON fr.forecast_id::text = g.forecast_id::text
    WHERE g._rn = 1
  $SQL$, v_union_sql);

  EXECUTE v_ins_sql;
  GET DIAGNOSTICS v_rows = ROW_COUNT;

  EXECUTE 'ANALYZE engine.tsf_vw_full_cache';

  RETURN QUERY SELECT 'engine.tsf_vw_full_cache'::text, COALESCE(v_rows,0);
END;
$$;

GRANT EXECUTE ON FUNCTION engine.rebake_summary_caches() TO aq_engine_owner, tsf_engine_app, matrix_reader;
