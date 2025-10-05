-- V14_12_Rebake_Summary_Caches.sql
-- Generated: 2025-10-04
-- Purpose: Rebuild summary caches for FULL views using the new CANNON-era schema.
-- Notes:
--   * Consumes all engine.*_vw_full_stage views produced by V14_11_TSF_Full_Views_Classical.sql.
--   * Writes to engine.tsf_vw_full_cache with the new columns (no variance/odds/sig; explicit CI fields; all 3 accuracy metrics).
--   * Selection ordering matches V14_11 final view: fv_mean_mape ASC, fv_mean_mape_c DESC, _src_ord ASC.

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
  -- Assemble UNION ALL from all per-table stage views already created by V14_11:
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

  -- Exact column list matches new cache definition from V14_11.
  v_ins_sql := format($SQL$
    INSERT INTO engine.tsf_vw_full_cache
      (forecast_id, forecast_name, date, value, model_name, fv,
       fv_mape, fv_mean_mape, fv_mean_mape_c,
       fv_mae,  fv_mean_mae,  fv_mean_mae_c,
       fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
       ci85_low, ci85_high,
       ci90_low, ci90_high,
       ci95_low, ci95_high,
       msr_dir, fmsr_dir, dir_hit, dir_hit_count)
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
      g.ci95_low, g.ci95_high,
      g.msr_dir, g.fmsr_dir, g.dir_hit, g.dir_hit_count
    FROM (
      SELECT
        y.*,
        row_number() OVER (
          PARTITION BY y.forecast_id, y.date
          ORDER BY
            y.fv_mean_mape ASC NULLS LAST,
            y.fv_mean_mape_c DESC NULLS LAST,
            y._src_ord ASC
        ) AS _rn
      FROM ( %s ) AS y
      WHERE y.fv IS NOT NULL AND y.fv <> 0 AND (y.fv_mean_mape_c IS NULL OR y.fv_mean_mape_c >= 5)
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
