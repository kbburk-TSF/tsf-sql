-- V4_07_SR_Series_SQ.sql
-- Milestone 2 (Zero-Arg & Disambiguated)
-- VC V4.07 (2025-09-17) — FIX: Wrapper CREATE TABLE column names aligned to V3 (smv/qsr/msr and q_* lookbacks/blends); added drop for existing *_core signature; no other changes.
--   * Derived from V3_07_SR_Series_SQ.sql; core logic preserved.
--   * Only change: destination tables are created if missing (type-safe, PK on (forecast_id,date), created_at).
--   * Added wrapper identical in pattern to V4_06 (zero-arg), with SQ column names.
BEGIN;

-- Clean out older overloads to avoid ambiguity
DROP FUNCTION IF EXISTS engine.build_sr_series_sq() CASCADE;
DROP FUNCTION IF EXISTS engine.build_sr_series_sq(uuid) CASCADE;
DROP FUNCTION IF EXISTS engine.build_sr_series_sq(uuid, uuid) CASCADE;

DROP FUNCTION IF EXISTS engine.build_sr_series_sq_core(uuid) CASCADE;
-- REPLACE: engine.build_sr_series_sq(uuid)
-- VC 1.1 (2025-09-12): PASS 2 optimized with set-based DISTINCT ON update; covering index
--                      (forecast_id, <model>_yqm, date) INCLUDE (<model>_qsr, <model>_msr); ANALYZE before update.
-- VC 2.0 (2025-09-13): OPT — session tuning, progress notices & timings, ANALYZE after PASS 2B and PASS 3 (logic unchanged).

BEGIN;
CREATE OR REPLACE FUNCTION engine.build_sr_series_sq_core(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  fid     uuid;
  mdl     record;
  out_tbl text;
  run_legacy_pass2 boolean := false;

  -- added for notices/timings
  _t_start timestamptz := clock_timestamp();
  _t_pass  timestamptz;
  _rows    bigint;
  t_main text;
  t_yqm text;
  t_k1 text;
  t_k2 text;
  t_k3 text;
BEGIN
  -- Session tuning (best-effort; ignored if not permitted)
  BEGIN
    PERFORM set_config('client_min_messages','NOTICE',true);
    PERFORM set_config('jit','off',true);
    PERFORM set_config('work_mem','256MB',true);
    PERFORM set_config('maintenance_work_mem','512MB',true);
    PERFORM set_config('max_parallel_workers_per_gather','4',true);
    PERFORM set_config('parallel_setup_cost','0',true);
    PERFORM set_config('parallel_tuple_cost','0',true);
    PERFORM set_config('synchronous_commit','off',true);
    PERFORM set_config('temp_buffers','64MB',true);
  EXCEPTION WHEN OTHERS THEN
    -- ignore
  END;

  RAISE NOTICE '[%] build_sr_series_sq — start', clock_timestamp();

  IF p_forecast_id IS NULL THEN
    SELECT ih.forecast_id INTO fid
    FROM engine.instance_historical ih
    GROUP BY ih.forecast_id
    ORDER BY MAX(ih.created_at) DESC NULLS LAST
    LIMIT 1;
  ELSE
    fid := p_forecast_id;
  END IF;
  IF fid IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found in engine.instance_historical.';
  END IF;

  RAISE NOTICE '[%] forecast_id = %', clock_timestamp(), fid;

  FOR mdl IN
    SELECT c.relname AS model
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='engine' AND c.relkind IN ('r','f')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name='date')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_q')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_q_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_q_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_q_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_sq';

    RAISE NOTICE '[%] SERIES % — target table engine.%', clock_timestamp(), mdl.model, out_tbl;

    
IF NOT EXISTS (
  SELECT 1 FROM information_schema.tables
  WHERE table_schema='engine' AND table_name=out_tbl
) THEN
  -- infer types from the model table
  SELECT
    (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_q')     AS t_main,
    (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_yqm')   AS t_yqm,
    (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_q_p1_k') AS t_k1,
    (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_q_p2_k') AS t_k2,
    (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_q_p3_k') AS t_k3
  INTO t_main, t_yqm, t_k1, t_k2, t_k3;

  EXECUTE format($ct$
    CREATE TABLE engine.%I (
      forecast_id uuid NOT NULL,
      date        date NOT NULL,
      value       double precision,
      qmv         double precision,
      mmv         double precision,
      %I %s,
      %I %s,
      %I double precision,
      %I double precision,
      %I double precision,
      %I %s,
      %I %s,
      %I %s,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      %I double precision,
      created_at timestamptz DEFAULT now(),
      PRIMARY KEY (forecast_id, date)
    );
  $ct$,
    out_tbl,
    mdl.model||'_q',     t_main,
    mdl.model||'_yqm',   t_yqm,
    mdl.model||'_smv',
    mdl.model||'_qsr',
    mdl.model||'_msr',
    mdl.model||'_q_p1_k', t_k1,
    mdl.model||'_q_p2_k', t_k2,
    mdl.model||'_q_p3_k', t_k3,
    mdl.model||'_q_p1_qsr',
    mdl.model||'_q_p1_msr',
    mdl.model||'_q_p2_qsr',
    mdl.model||'_q_p2_msr',
    mdl.model||'_q_p3_qsr',
    mdl.model||'_q_p3_msr',
    mdl.model||'_q_fqsr_a1',
    mdl.model||'_q_fqsr_a2',
    mdl.model||'_q_fqsr_a2w',
    mdl.model||'_q_fqsr_a3',
    mdl.model||'_q_fqsr_a3w',
    mdl.model||'_q_fmsr_a1',
    mdl.model||'_q_fmsr_a2',
    mdl.model||'_q_fmsr_a2w',
    mdl.model||'_q_fmsr_a3',
    mdl.model||'_q_fmsr_a3w'
  );
END IF;


    -- PASS 1: base insert with UPSERT
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 1 — insert/upsert into engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      INSERT INTO engine.%1$I (
        forecast_id, date, value, qmv, mmv,
        %2$I, %3$I, %4$I, %5$I, %6$I,
        %7$I, %8$I, %9$I
      )
      SELECT
        %10$L::uuid,
        h.date::date,
        h.value, h.qmv, h.mmv,
        m.%2$I, m.%3$I,
        avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I)                              AS %4$I,
        (avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I) / NULLIF(h.qmv,0))           AS %5$I,
        (avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I) / NULLIF(h.mmv,0))           AS %6$I,
        m.%7$I, m.%8$I, m.%9$I
      FROM engine.instance_historical h
      JOIN engine.%11$I m ON m.date = h.date
      WHERE h.forecast_id = %10$L
      ORDER BY h.date
      ON CONFLICT (forecast_id, date) DO UPDATE SET
        value   = EXCLUDED.value,
        qmv     = EXCLUDED.qmv,
        mmv     = EXCLUDED.mmv,
        %2$I    = EXCLUDED.%2$I,
        %3$I    = EXCLUDED.%3$I,
        %4$I    = EXCLUDED.%4$I,
        %5$I    = EXCLUDED.%5$I,
        %6$I    = EXCLUDED.%6$I,
        %7$I    = EXCLUDED.%7$I,
        %8$I    = EXCLUDED.%8$I,
        %9$I    = EXCLUDED.%9$I
      ;
    $q$,
      out_tbl,
      mdl.model||'_q',               -- %2
      mdl.model||'_yqm',             -- %3
      mdl.model||'_smv',             -- %4
      mdl.model||'_qsr',             -- %5
      mdl.model||'_msr',             -- %6
      mdl.model||'_q_p1_k',          -- %7
      mdl.model||'_q_p2_k',          -- %8
      mdl.model||'_q_p3_k',          -- %9
      fid,                           -- %10
      mdl.model                      -- %11
    );
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 1 done — rows affected: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON engine.%I (%I, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_sq_fid_yqm_date',
      out_tbl,
      'forecast_id', mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );
    EXECUTE format('ANALYZE engine.%I', out_tbl);

    -- PASS 2 legacy (kept, disabled)
    IF run_legacy_pass2 THEN
      _t_pass := clock_timestamp();
      RAISE NOTICE '[%] PASS 2 (legacy) — engine.%', _t_pass, out_tbl;

      EXECUTE format($q$
        UPDATE engine.%1$I AS t
        SET
          %2$I = (SELECT p.%6$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%5$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %3$I = (SELECT p.%7$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%5$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),

          %8$I = (SELECT p.%6$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%9$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %10$I= (SELECT p.%7$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%9$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),

          %11$I= (SELECT p.%6$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%12$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %13$I= (SELECT p.%7$I FROM engine.%1$I p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%12$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1)
        WHERE t.forecast_id = %14$L;
      $q$,
        out_tbl,
        mdl.model||'_q_p1_qsr', mdl.model||'_q_p1_msr',
        mdl.model||'_yqm',      mdl.model||'_q_p1_k',
        mdl.model||'_qsr',      mdl.model||'_msr',
        mdl.model||'_q_p2_qsr', mdl.model||'_q_p2_k', mdl.model||'_q_p2_msr',
        mdl.model||'_q_p3_qsr', mdl.model||'_q_p3_k', mdl.model||'_q_p3_msr',
        fid
      );

      GET DIAGNOSTICS _rows = ROW_COUNT;
      RAISE NOTICE '[%] PASS 2 (legacy) done — rows updated: %, elapsed: %.3f s',
        clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);
    END IF;

    -- PASS 2B optimized
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 2B — lookbacks via DISTINCT ON — engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      WITH
      p1 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM engine.%1$I t
        JOIN engine.%1$I s
          ON s.forecast_id = t.forecast_id
         AND s.%4$I        = t.%5$I
         AND s.date        < t.date
        WHERE t.forecast_id = %14$L
        ORDER BY t.forecast_id, t.date, s.date DESC
      ),
      p2 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM engine.%1$I t
        JOIN engine.%1$I s
          ON s.forecast_id = t.forecast_id
         AND s.%4$I        = t.%9$I
         AND s.date        < t.date
        WHERE t.forecast_id = %14$L
        ORDER BY t.forecast_id, t.date, s.date DESC
      ),
      p3 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM engine.%1$I t
        JOIN engine.%1$I s
          ON s.forecast_id = t.forecast_id
         AND s.%4$I        = t.%12$I
         AND s.date        < t.date
        WHERE t.forecast_id = %14$L
        ORDER BY t.forecast_id, t.date, s.date DESC
      ),
      keys AS (
        SELECT forecast_id, date FROM p1
        UNION
        SELECT forecast_id, date FROM p2
        UNION
        SELECT forecast_id, date FROM p3
      ),
      allp AS (
        SELECT k.forecast_id, k.date,
               p1.qsr AS p1_qsr, p1.msr AS p1_msr,
               p2.qsr AS p2_qsr, p2.msr AS p2_msr,
               p3.qsr AS p3_qsr, p3.msr AS p3_msr
        FROM keys k
        LEFT JOIN p1 ON p1.forecast_id=k.forecast_id AND p1.date=k.date
        LEFT JOIN p2 ON p2.forecast_id=k.forecast_id AND p2.date=k.date
        LEFT JOIN p3 ON p3.forecast_id=k.forecast_id AND p3.date=k.date
      )
      UPDATE engine.%1$I t
         SET %2$I  = allp.p1_qsr,
             %3$I  = allp.p1_msr,
             %8$I  = allp.p2_qsr,
             %10$I = allp.p2_msr,
             %11$I = allp.p3_qsr,
             %13$I = allp.p3_msr
      FROM allp
      WHERE t.forecast_id = allp.forecast_id
        AND t.date        = allp.date
        AND t.forecast_id = %14$L;
    $q$,
      out_tbl,
      mdl.model||'_q_p1_qsr', mdl.model||'_q_p1_msr',
      mdl.model||'_yqm',      mdl.model||'_q_p1_k',
      mdl.model||'_qsr',      mdl.model||'_msr',
      mdl.model||'_q_p2_qsr', mdl.model||'_q_p2_k', mdl.model||'_q_p2_msr',
      mdl.model||'_q_p3_qsr', mdl.model||'_q_p3_k', mdl.model||'_q_p3_msr',
      fid
    );
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 2B done — rows updated: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- ANALYZE after PASS 2B (added)
    EXECUTE format('ANALYZE engine.%I', out_tbl);

    -- PASS 3: blends (unchanged)
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 3 — blends — engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      UPDATE engine.%2$I t
      SET
        %1$s_q_fqsr_a1  = ((%1$s_q_p1_qsr + %1$s_q_p2_qsr)/2.0),
        %1$s_q_fqsr_a2  = ((%1$s_q_p1_qsr * 0.75) + (%1$s_q_p2_qsr * 0.25)),
        %1$s_q_fqsr_a2w = ((%1$s_q_p1_qsr * 0.75) + (%1$s_q_p2_qsr * 0.25)),
        %1$s_q_fqsr_a3  = ((%1$s_q_p1_qsr + %1$s_q_p2_qsr + %1$s_q_p3_qsr)/3.0),
        %1$s_q_fqsr_a3w = ((%1$s_q_p1_qsr * 0.5) + (%1$s_q_p2_qsr * 0.3) + (%1$s_q_p3_qsr * 0.2)),

        %1$s_q_fmsr_a1  = ((%1$s_q_p1_msr + %1$s_q_p2_msr)/2.0),
        %1$s_q_fmsr_a2  = ((%1$s_q_p1_msr * 0.75) + (%1$s_q_p2_msr * 0.25)),
        %1$s_q_fmsr_a2w = ((%1$s_q_p1_msr * 0.75) + (%1$s_q_p2_msr * 0.25)),
        %1$s_q_fmsr_a3  = ((%1$s_q_p1_msr + %1$s_q_p2_msr + %1$s_q_p3_msr)/3.0),
        %1$s_q_fmsr_a3w = ((%1$s_q_p1_msr * 0.5) + (%1$s_q_p2_msr * 0.3) + (%1$s_q_p3_msr * 0.2))
      WHERE t.forecast_id = %3$L;
    $q$, mdl.model, out_tbl, fid);
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 3 done — rows updated: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- ANALYZE after PASS 3 (added)
    EXECUTE format('ANALYZE engine.%I', out_tbl);

    RAISE NOTICE '[%] SERIES % — complete', clock_timestamp(), mdl.model;
  END LOOP;

  RAISE NOTICE '[%] build_sr_series_sq — done (elapsed %.3f s)', clock_timestamp(), EXTRACT(epoch FROM clock_timestamp() - _t_start);
END;
$$;
COMMIT;

-- ===================== ZERO-ARG WRAPPER (creates dest tables, no math changes) — SQ =====================
CREATE OR REPLACE FUNCTION engine.build_sr_series_sq()
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  fid uuid;
  mdl record;
  out_tbl text;
  col_base text;  -- <model>
  col_q    text;  -- <model>_q
  col_yqm  text;  -- <model>_yqm
  col_k1   text;  -- <model>_q_p1_k
  col_k2   text;  -- <model>_q_p2_k
  col_k3   text;  -- <model>_q_p3_k
  t_q   text;
  t_yqm text;
  t_k1  text;
  t_k2  text;
  t_k3  text;
BEGIN
  SELECT ih.forecast_id
  INTO fid
  FROM engine.instance_historical ih
  GROUP BY ih.forecast_id
  ORDER BY MAX(ih.created_at) DESC NULLS LAST
  LIMIT 1;

  IF fid IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found in engine.instance_historical.';
  END IF;

  FOR mdl IN
    SELECT c.relname AS model
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='engine' AND c.relkind IN ('r','f')
      AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=c.relname AND column_name='date')
  LOOP
    out_tbl := mdl.model || '_instance_sr_sq';

    col_base := mdl.model;
    col_q    := mdl.model||'_q';
    col_yqm  := mdl.model||'_yqm';
    col_k1   := mdl.model||'_q_p1_k';
    col_k2   := mdl.model||'_q_p2_k';
    col_k3   := mdl.model||'_q_p3_k';

    -- ensure required driver columns exist on model table
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=mdl.model AND column_name=col_q) THEN
      CONTINUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=mdl.model AND column_name=col_yqm) THEN
      CONTINUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=mdl.model AND column_name=col_k1) THEN
      CONTINUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=mdl.model AND column_name=col_k2) THEN
      CONTINUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='engine' AND table_name=mdl.model AND column_name=col_k3) THEN
      CONTINUE;
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
      SELECT
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=col_q)    AS t_q,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=col_yqm)  AS t_yqm,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=col_k1)   AS t_k1,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=col_k2)   AS t_k2,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=col_k3)   AS t_k3
      INTO t_q, t_yqm, t_k1, t_k2, t_k3;

      EXECUTE format($ct$
        CREATE TABLE engine.%I (
          forecast_id uuid NOT NULL,
          date        date NOT NULL,
          value       double precision,
          qmv         double precision,
          mmv         double precision,
          %I %s,                  -- <model>_q
          %I %s,                  -- <model>_yqm
          %I double precision,    -- <model>_smv
          %I double precision,    -- <model>_qsr
          %I double precision,    -- <model>_msr
          %I %s,                  -- <model>_q_p1_k
          %I %s,                  -- <model>_q_p2_k
          %I %s,                  -- <model>_q_p3_k
          %I double precision,    -- <model>_q_p1_qsr
          %I double precision,    -- <model>_q_p1_msr
          %I double precision,    -- <model>_q_p2_qsr
          %I double precision,    -- <model>_q_p2_msr
          %I double precision,    -- <model>_q_p3_qsr
          %I double precision,    -- <model>_q_p3_msr
          %I double precision,    -- <model>_q_fqsr_a1
          %I double precision,    -- <model>_q_fqsr_a2
          %I double precision,    -- <model>_q_fqsr_a2w
          %I double precision,    -- <model>_q_fqsr_a3
          %I double precision,    -- <model>_q_fqsr_a3w
          %I double precision,    -- <model>_q_fmsr_a1
          %I double precision,    -- <model>_q_fmsr_a2
          %I double precision,    -- <model>_q_fmsr_a2w
          %I double precision,    -- <model>_q_fmsr_a3
          %I double precision,    -- <model>_q_fmsr_a3w
          created_at timestamptz DEFAULT now(),
          PRIMARY KEY (forecast_id, date)
        )
      $ct$,
        out_tbl,
        col_q,   t_q,
        col_yqm, t_yqm,
        col_base||'_smv',
        col_base||'_qsr',
        col_base||'_msr',
        col_k1,  t_k1,
        col_k2,  t_k2,
        col_k3,  t_k3,
        col_base||'_q_p1_qsr',
        col_base||'_q_p1_msr',
        col_base||'_q_p2_qsr',
        col_base||'_q_p2_msr',
        col_base||'_q_p3_qsr',
        col_base||'_q_p3_msr',
        col_base||'_q_fqsr_a1',
        col_base||'_q_fqsr_a2',
        col_base||'_q_fqsr_a2w',
        col_base||'_q_fqsr_a3',
        col_base||'_q_fqsr_a3w',
        col_base||'_q_fmsr_a1',
        col_base||'_q_fmsr_a2',
        col_base||'_q_fmsr_a2w',
        col_base||'_q_fmsr_a3',
        col_base||'_q_fmsr_a3w'
      );

      EXECUTE format('GRANT SELECT ON engine.%I TO matrix_reader', out_tbl);
      EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO tsf_engine_app', out_tbl);
    END IF;
  END LOOP;

  PERFORM engine.build_sr_series_sq_core(fid);
END;
$$;

GRANT EXECUTE ON FUNCTION engine.build_sr_series_sq() TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.build_sr_series_sq_core(uuid) TO matrix_reader, tsf_engine_app;

COMMIT;
