-- ===================================================================
-- File: V12_15_Classical_Forecast_Views_3globals_MAPE_25p.sql
-- Updated: 2025-09-24
-- CHANGE: MAE→MAPE column cascade; rank by lowest fv_mean_mape;
--         enforce fv_mean_mape_c >= 5 at all levels.
-- ===================================================================


-- ===================================================================
-- File: V11_15_Classical_Forecast_Views_3globals.sql
-- Updated: 2025-09-24 19:35
-- CHANGE: Correct PL/pgSQL quoting and enforce fv_mean_mape_c >= 5
--         before ranking/selection at ALL levels.
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

-- discover forecast tables
DROP TABLE IF EXISTS pg_temp._src;
CREATE TEMP TABLE pg_temp._src AS
SELECT
  c.relname AS table_name,
  CASE
    WHEN c.relname ILIKE '%\_forecast_msqm' ESCAPE '\' THEN 'msqm'
    WHEN c.relname ILIKE '%\_forecast_msq'  ESCAPE '\' THEN 'msq'
    ELSE 'ms'
  END AS series
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'engine'
  AND c.relkind = 'r'
  AND c.relname ILIKE '%\_instance\_forecast\_%' ESCAPE '\'
ORDER BY 1;

-- util
CREATE OR REPLACE FUNCTION engine._drop_view_if_exists(v_schema text, v_view text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = v_schema AND viewname = v_view) THEN
    EXECUTE 'DROP VIEW ' || quote_ident(v_schema) || '.' || quote_ident(v_view);
  END IF;
END$$;

-- per-table filtered views (fv_mean_mape_c >= 5)
DO $$
DECLARE
  r record;
  has_model_name boolean;
  has_interval  boolean;
  sel_cols text;
  viewname text;
  sql text;
BEGIN
  FOR r IN SELECT * FROM pg_temp._src LOOP
    SELECT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name=r.table_name AND column_name='model_name'
           ) INTO has_model_name;
    SELECT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name=r.table_name AND column_name='fv_interval_odds'
           ) INTO has_interval;

    sel_cols :=
      't.forecast_id,'||
      't.date,'||
      't.value,'||
      't.fv_l,'||
      't.fv,'||
      't.fv_u,'||
      't.fv_mean_mape,'||
      (CASE WHEN has_interval   THEN 't.fv_interval_odds' ELSE 'NULL::numeric AS fv_interval_odds' END)||','||
      (CASE WHEN has_model_name THEN 't.model_name'       ELSE 'NULL::text AS model_name' END)||','||
      'regexp_replace('||quote_literal(r.table_name)||',''_instance_forecast_.*$'','''')::text AS model_key,'||
      quote_literal(r.series)||'::text AS series,'||
      quote_literal(r.table_name)||'::text AS table_name,'||
      'fr.forecast_name';

    -- ARIMA_A0
    viewname := r.table_name || '_vw_arima_a0';
    PERFORM engine._drop_view_if_exists('engine', viewname);
    sql := 'CREATE OR REPLACE VIEW engine.'||quote_ident(viewname)||' AS '||
           'SELECT '||sel_cols||' FROM engine.'||quote_ident(r.table_name)||' t '||
           'LEFT JOIN engine.forecast_registry fr ON fr.forecast_id::text = t.forecast_id::text '||
           'WHERE t.base_model = ''arima_m'' AND t.fmsr_series = ''A0'' AND t.fv_mean_mape_c >= 5;';
    EXECUTE sql;

    -- SES_A0
    viewname := r.table_name || '_vw_ses_a0';
    PERFORM engine._drop_view_if_exists('engine', viewname);
    sql := 'CREATE OR REPLACE VIEW engine.'||quote_ident(viewname)||' AS '||
           'SELECT '||sel_cols||' FROM engine.'||quote_ident(r.table_name)||' t '||
           'LEFT JOIN engine.forecast_registry fr ON fr.forecast_id::text = t.forecast_id::text '||
           'WHERE t.base_model = ''ses_m'' AND t.fmsr_series = ''A0'' AND t.fv_mean_mape_c >= 5;';
    EXECUTE sql;

    -- HWES_A0
    viewname := r.table_name || '_vw_hwes_a0';
    PERFORM engine._drop_view_if_exists('engine', viewname);
    sql := 'CREATE OR REPLACE VIEW engine.'||quote_ident(viewname)||' AS '||
           'SELECT '||sel_cols||' FROM engine.'||quote_ident(r.table_name)||' t '||
           'LEFT JOIN engine.forecast_registry fr ON fr.forecast_id::text = t.forecast_id::text '||
           'WHERE t.base_model = ''hwes_m'' AND t.fmsr_series = ''A0'' AND t.fv_mean_mape_c >= 5;';
    EXECUTE sql;
  END LOOP;
END$$ LANGUAGE plpgsql;

-- global sources (fv_mean_mape_c >= 5)
DO $$
DECLARE
  r record;
  has_model_name boolean;
  has_interval  boolean;
  piece text;
  u_arima text := NULL;
  u_ses   text := NULL;
  u_hwes  text := NULL;
  sql text;
BEGIN
  FOR r IN SELECT * FROM pg_temp._src LOOP
    SELECT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name=r.table_name AND column_name='model_name'
           ) INTO has_model_name;
    SELECT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name=r.table_name AND column_name='fv_interval_odds'
           ) INTO has_interval;

    piece :=
      'SELECT '||
      't.forecast_id, t.date, t.value, t.fv_l, t.fv, t.fv_u, t.fv_mean_mape, '||
      (CASE WHEN has_interval   THEN 't.fv_interval_odds' ELSE 'NULL::numeric AS fv_interval_odds' END)||', '||
      (CASE WHEN has_model_name THEN 't.model_name'       ELSE 'NULL::text AS model_name' END)||', '||
      'regexp_replace('||quote_literal(r.table_name)||',''_instance_forecast_.*$'','''')::text AS model_key, '||
      quote_literal(r.series)||'::text AS series, '||
      quote_literal(r.table_name)||'::text AS table_name, '||
      'fr.forecast_name '||
      'FROM engine.'||quote_ident(r.table_name)||' t '||
      'LEFT JOIN engine.forecast_registry fr ON fr.forecast_id::text = t.forecast_id::text '||
      'WHERE t.fmsr_series = ''A0'' AND t.fv_mean_mape_c >= 5 AND t.base_model = ';

    u_arima := coalesce(u_arima||E'\nUNION ALL\n','') || piece || quote_literal('arima_m');
    u_ses   := coalesce(u_ses  ||E'\nUNION ALL\n','') || piece || quote_literal('ses_m');
    u_hwes  := coalesce(u_hwes ||E'\nUNION ALL\n','') || piece || quote_literal('hwes_m');
  END LOOP;

  IF u_arima IS NOT NULL AND left(u_arima, 11) = 'UNION ALL\n' THEN u_arima := substr(u_arima, 12); END IF;
  IF u_ses   IS NOT NULL AND left(u_ses,   11) = 'UNION ALL\n' THEN u_ses   := substr(u_ses,   12); END IF;
  IF u_hwes  IS NOT NULL AND left(u_hwes,  11) = 'UNION ALL\n' THEN u_hwes  := substr(u_hwes,  12); END IF;

  PERFORM engine._drop_view_if_exists('engine','tsf_vw_daily_best_arima_a0_src');
  sql := 'CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_arima_a0_src AS '||
         'WITH universe AS ('||u_arima||'), '||
         'ranked AS (SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.date '||
         'ORDER BY u.fv_mean_mape NULLS LAST, u.fv_interval_odds NULLS LAST, (u.fv_u - u.fv_l) NULLS LAST, u.table_name) AS _rn FROM universe u) '||
         'SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name '||
         'FROM ranked WHERE _rn = 1;';
  EXECUTE sql;

  PERFORM engine._drop_view_if_exists('engine','tsf_vw_daily_best_ses_a0_src');
  sql := 'CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_ses_a0_src AS '||
         'WITH universe AS ('||u_ses||'), '||
         'ranked AS (SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.date '||
         'ORDER BY u.fv_mean_mape NULLS LAST, u.fv_interval_odds NULLS LAST, (u.fv_u - u.fv_l) NULLS LAST, u.table_name) AS _rn FROM universe u) '||
         'SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name '||
         'FROM ranked WHERE _rn = 1;';
  EXECUTE sql;

  PERFORM engine._drop_view_if_exists('engine','tsf_vw_daily_best_hwes_a0_src');
  sql := 'CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_hwes_a0_src AS '||
         'WITH universe AS ('||u_hwes||'), '||
         'ranked AS (SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.date '||
         'ORDER BY u.fv_mean_mape NULLS LAST, u.fv_interval_odds NULLS LAST, (u.fv_u - u.fv_l) NULLS LAST, u.table_name) AS _rn FROM universe u) '||
         'SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name '||
         'FROM ranked WHERE _rn = 1;';
  EXECUTE sql;
END$$ LANGUAGE plpgsql;

-- caches + backfill + swap
DROP TABLE IF EXISTS engine.tsf_vw_daily_best_arima_a0_cache CASCADE;
CREATE TABLE engine.tsf_vw_daily_best_arima_a0_cache(
  forecast_id uuid,
  date date NOT NULL,
  value numeric,
  fv_l numeric,
  fv numeric,
  fv_u numeric,
  fv_mean_mape numeric,
  fv_interval_odds numeric,
  model_name text,
  model_key text,
  series text,
  table_name text,
  forecast_name text,
  CONSTRAINT tsf_vw_daily_best_arima_a0_cache_uk UNIQUE (date)
);
CREATE INDEX tsf_vw_daily_best_arima_a0_cache_ix_date ON engine.tsf_vw_daily_best_arima_a0_cache(date);

DROP TABLE IF EXISTS engine.tsf_vw_daily_best_ses_a0_cache CASCADE;
CREATE TABLE engine.tsf_vw_daily_best_ses_a0_cache(
  forecast_id uuid,
  date date NOT NULL,
  value numeric,
  fv_l numeric,
  fv numeric,
  fv_u numeric,
  fv_mean_mape numeric,
  fv_interval_odds numeric,
  model_name text,
  model_key text,
  series text,
  table_name text,
  forecast_name text,
  CONSTRAINT tsf_vw_daily_best_ses_a0_cache_uk UNIQUE (date)
);
CREATE INDEX tsf_vw_daily_best_ses_a0_cache_ix_date ON engine.tsf_vw_daily_best_ses_a0_cache(date);

DROP TABLE IF EXISTS engine.tsf_vw_daily_best_hwes_a0_cache CASCADE;
CREATE TABLE engine.tsf_vw_daily_best_hwes_a0_cache(
  forecast_id uuid,
  date date NOT NULL,
  value numeric,
  fv_l numeric,
  fv numeric,
  fv_u numeric,
  fv_mean_mape numeric,
  fv_interval_odds numeric,
  model_name text,
  model_key text,
  series text,
  table_name text,
  forecast_name text,
  CONSTRAINT tsf_vw_daily_best_hwes_a0_cache_uk UNIQUE (date)
);
CREATE INDEX tsf_vw_daily_best_hwes_a0_cache_ix_date ON engine.tsf_vw_daily_best_hwes_a0_cache(date);

TRUNCATE engine.tsf_vw_daily_best_arima_a0_cache;
INSERT INTO engine.tsf_vw_daily_best_arima_a0_cache SELECT * FROM engine.tsf_vw_daily_best_arima_a0_src;
TRUNCATE engine.tsf_vw_daily_best_ses_a0_cache;
INSERT INTO engine.tsf_vw_daily_best_ses_a0_cache SELECT * FROM engine.tsf_vw_daily_best_ses_a0_src;
TRUNCATE engine.tsf_vw_daily_best_hwes_a0_cache;
INSERT INTO engine.tsf_vw_daily_best_hwes_a0_cache SELECT * FROM engine.tsf_vw_daily_best_hwes_a0_src;

CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_arima_a0 AS
SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name
FROM engine.tsf_vw_daily_best_arima_a0_cache;

CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_ses_a0 AS
SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name
FROM engine.tsf_vw_daily_best_ses_a0_cache;

CREATE OR REPLACE VIEW engine.tsf_vw_daily_best_hwes_a0 AS
SELECT forecast_id, date, value, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, model_name, model_key, series, table_name, forecast_name
FROM engine.tsf_vw_daily_best_hwes_a0_cache;

INSERT INTO engine.prebaked_views(view_name) VALUES
  ('engine.tsf_vw_daily_best_arima_a0'),
  ('engine.tsf_vw_daily_best_ses_a0'),
  ('engine.tsf_vw_daily_best_hwes_a0')
ON CONFLICT (view_name) DO NOTHING;

DROP FUNCTION IF EXISTS engine._drop_view_if_exists(text, text);

COMMIT;
-- ============================ END OF FILE ============================
