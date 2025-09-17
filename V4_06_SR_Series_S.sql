-- V4_06_SR_Series_S.sql
-- Milestone 2: preserve original SR S-series logic and add wrapper that creates destination tables if missing.
-- VC V4.0 (2025-09-17): 
--   * Original function engine.build_sr_series_s(uuid) kept verbatim below.
--   * New wrapper engine.build_sr_series_s(uuid, uuid) ensures destination tables exist (no math changes), then calls original.
--   * Grants aligned to matrix_reader and tsf_engine_app.

-- FUNCTION: engine.build_sr_series_s(uuid) — wraps your SR Series S cannon into a callable function
-- - Does NOT create/alter destination tables; raises if missing
-- - Uses the passed forecast_id; if NULL, falls back to latest by created_at
-- REPLACE: engine.build_sr_series_s(uuid)
-- VC 1.1 (2025-09-12): PASS 2 optimized with set-based DISTINCT ON update; adds covering index
--                      (forecast_id, <model>_yqm, date) INCLUDE (<model>_qsr, <model>_msr); ANALYZE before update.
-- VC 2.0 (2025-09-13): OPT — session tuning, progress notices & timings, ANALYZE after PASS 2B and PASS 3 (logic unchanged).

BEGIN;
CREATE OR REPLACE FUNCTION engine.build_sr_series_s(p_forecast_id uuid DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  fid     uuid;
  mdl     record;
  out_tbl text;
  run_legacy_pass2 boolean := false; -- keep old PASS 2 for reference; do not execute

  -- added for notices/timings
  _t_start timestamptz := clock_timestamp();
  _t_pass  timestamptz;
  _rows    bigint;
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
    -- ignore: not all settings are allowed on all hosts
  END;

  RAISE NOTICE '[%] build_sr_series_s — start', clock_timestamp();

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
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname)
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_s';

    RAISE NOTICE '[%] SERIES % — target table engine.%', clock_timestamp(), mdl.model, out_tbl;

    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
      RAISE EXCEPTION 'Target table engine.% does not exist. Create it first.', out_tbl;
    END IF;

    -- PASS 1: base insert with UPSERT (idempotent)
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
      mdl.model,                     -- %2
      mdl.model||'_yqm',             -- %3
      mdl.model||'_smv',             -- %4
      mdl.model||'_qsr',             -- %5
      mdl.model||'_msr',             -- %6
      mdl.model||'_p1_k',            -- %7
      mdl.model||'_p2_k',            -- %8
      mdl.model||'_p3_k',            -- %9
      fid,                           -- %10
      mdl.model                      -- %11
    );
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 1 done — rows affected: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- Covering index to accelerate PASS 2B lookbacks
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON engine.%I (%I, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_s_fid_yqm_date',
      out_tbl,
      'forecast_id', mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );

    -- Analyze so planner uses fresh stats/index
    EXECUTE format('ANALYZE engine.%I', out_tbl);

    -- PASS 2 (legacy; preserved, disabled)
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
        mdl.model||'_p1_qsr', mdl.model||'_p1_msr',
        mdl.model||'_yqm',    mdl.model||'_p1_k',
        mdl.model||'_qsr',    mdl.model||'_msr',
        mdl.model||'_p2_qsr', mdl.model||'_p2_k', mdl.model||'_p2_msr',
        mdl.model||'_p3_qsr', mdl.model||'_p3_k', mdl.model||'_p3_msr',
        fid
      );

      GET DIAGNOSTICS _rows = ROW_COUNT;
      RAISE NOTICE '[%] PASS 2 (legacy) done — rows updated: %, elapsed: %.3f s',
        clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);
    END IF;

    -- PASS 2B (optimized): single set-based update using DISTINCT ON per (forecast_id,date)
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 2B — lookbacks via DISTINCT ON — engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      WITH
      p1 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date,
               s.%6$I AS qsr, s.%7$I AS msr
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
               t.forecast_id, t.date,
               s.%6$I AS qsr, s.%7$I AS msr
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
               t.forecast_id, t.date,
               s.%6$I AS qsr, s.%7$I AS msr
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
      mdl.model||'_p1_qsr', mdl.model||'_p1_msr',
      mdl.model||'_yqm',    mdl.model||'_p1_k',
      mdl.model||'_qsr',    mdl.model||'_msr',
      mdl.model||'_p2_qsr', mdl.model||'_p2_k', mdl.model||'_p2_msr',
      mdl.model||'_p3_qsr', mdl.model||'_p3_k', mdl.model||'_p3_msr',
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
        %1$s_fqsr_a1  = ((%1$s_p1_qsr + %1$s_p2_qsr)/2.0),
        %1$s_fqsr_a2  = ((%1$s_p1_qsr * 0.75) + (%1$s_p2_qsr * 0.25)),
        %1$s_fqsr_a2w = ((%1$s_p1_qsr * 0.75) + (%1$s_p2_qsr * 0.25)),
        %1$s_fqsr_a3  = ((%1$s_p1_qsr + %1$s_p2_qsr + %1$s_p3_qsr)/3.0),
        %1$s_fqsr_a3w = ((%1$s_p1_qsr * 0.5) + (%1$s_p2_qsr * 0.3) + (%1$s_p3_qsr * 0.2)),

        %1$s_fmsr_a1  = ((%1$s_p1_msr + %1$s_p2_msr)/2.0),
        %1$s_fmsr_a2  = ((%1$s_p1_msr * 0.75) + (%1$s_p2_msr * 0.25)),
        %1$s_fmsr_a2w = ((%1$s_p1_msr * 0.75) + (%1$s_p2_msr * 0.25)),
        %1$s_fmsr_a3  = ((%1$s_p1_msr + %1$s_p2_msr + %1$s_p3_msr)/3.0),
        %1$s_fmsr_a3w = ((%1$s_p1_msr * 0.5) + (%1$s_p2_msr * 0.3) + (%1$s_p3_msr * 0.2))
      WHERE t.forecast_id = %3$L;
    $q$, mdl.model, out_tbl, fid);
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 3 done — rows updated: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- ANALYZE after PASS 3 (added)
    EXECUTE format('ANALYZE engine.%I', out_tbl);

    RAISE NOTICE '[%] SERIES % — complete', clock_timestamp(), mdl.model;
  END LOOP;

  RAISE NOTICE '[%] build_sr_series_s — done (elapsed %.3f s)', clock_timestamp(), EXTRACT(epoch FROM clock_timestamp() - _t_start);
END;
$$;
COMMIT;


-- ===================== PIPELINE WRAPPER (no math changes) =====================
-- Ensures destination tables exist, then calls the original SR S-series function.
CREATE OR REPLACE FUNCTION engine.build_sr_series_s(
  p_forecast_id uuid,
  p_run_id uuid DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  fid uuid;
  mdl record;
  out_tbl text;
  t_model text;
  t_yqm  text;
  t_k1   text;
  t_k2   text;
  t_k3   text;
BEGIN
  -- Determine forecast_id (same selection rule as the original function)
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

  -- Create missing destination tables per model (types copied from source model columns)
  FOR mdl IN
    SELECT c.relname AS model
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='engine' AND c.relkind IN ('r','f')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name='date')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname)
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_s';

    -- If the table exists, move on; otherwise create it with correct types and PK
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
      -- Determine source types for key columns using format_type()
      SELECT
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
         JOIN pg_class cc ON cc.oid = a.attrelid
         JOIN pg_namespace nn ON nn.oid = cc.relnamespace
         WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model) AS t_model,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
         JOIN pg_class cc ON cc.oid = a.attrelid
         JOIN pg_namespace nn ON nn.oid = cc.relnamespace
         WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_yqm') AS t_yqm,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
         JOIN pg_class cc ON cc.oid = a.attrelid
         JOIN pg_namespace nn ON nn.oid = cc.relnamespace
         WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_p1_k') AS t_k1,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
         JOIN pg_class cc ON cc.oid = a.attrelid
         JOIN pg_namespace nn ON nn.oid = cc.relnamespace
         WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_p2_k') AS t_k2,
        (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
         JOIN pg_class cc ON cc.oid = a.attrelid
         JOIN pg_namespace nn ON nn.oid = cc.relnamespace
         WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=mdl.model||'_p3_k') AS t_k3
      INTO t_model, t_yqm, t_k1, t_k2, t_k3;

      EXECUTE format($ct$
        CREATE TABLE engine.%1$I (
          forecast_id uuid NOT NULL,
          date        date NOT NULL,
          value       double precision,
          qmv         double precision,
          mmv         double precision,
          %2$I        %3$s,
          %4$I        %5$s,
          %2$I||'_smv' double precision,
          %2$I||'_qsr' double precision,
          %2$I||'_msr' double precision,
          %6$I        %7$s,
          %8$I        %9$s,
          %10$I       %11$s,
          %2$I||'_p1_qsr' double precision,
          %2$I||'_p1_msr' double precision,
          %2$I||'_p2_qsr' double precision,
          %2$I||'_p2_msr' double precision,
          %2$I||'_p3_qsr' double precision,
          %2$I||'_p3_msr' double precision,
          %2$I||'_fqsr_a1'  double precision,
          %2$I||'_fqsr_a2'  double precision,
          %2$I||'_fqsr_a2w' double precision,
          %2$I||'_fqsr_a3'  double precision,
          %2$I||'_fqsr_a3w' double precision,
          %2$I||'_fmsr_a1'  double precision,
          %2$I||'_fmsr_a2'  double precision,
          %2$I||'_fmsr_a2w' double precision,
          %2$I||'_fmsr_a3'  double precision,
          %2$I||'_fmsr_a3w' double precision,
          created_at timestamptz DEFAULT now(),
          PRIMARY KEY (forecast_id, date)
        );
      $ct$,
        out_tbl,
        mdl.model, t_model,
        mdl.model||'_yqm', t_yqm,
        mdl.model||'_p1_k', t_k1,
        mdl.model||'_p2_k', t_k2,
        mdl.model||'_p3_k', t_k3
      );

      -- Visibility & app access
      EXECUTE format('GRANT SELECT ON engine.%I TO matrix_reader', out_tbl);
      EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO tsf_engine_app', out_tbl);
    END IF;
  END LOOP;

  -- Call the original SR S-series function (contains all math/logic)
  PERFORM engine.build_sr_series_s(fid);

END;
$$;

-- Execution privileges for both signatures
GRANT EXECUTE ON FUNCTION engine.build_sr_series_s(uuid) TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.build_sr_series_s(uuid, uuid) TO matrix_reader, tsf_engine_app;
