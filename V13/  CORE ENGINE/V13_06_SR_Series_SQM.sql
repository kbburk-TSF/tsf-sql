-- V4_08_SR_Series_SQM.sql
-- 2025-09-27: FIX — __universe now slices from engine.instance_historical (typo corrected).
-- 2025-09-27: PERF — universal segmenting: __universe + TEMP __work; all passes operate on __work; final upsert to dest. No math changes.
-- Milestone 2 (Zero-Arg & Disambiguated)
-- VC V4.08 (2025-09-17) — FIX: Wrapper pattern matched to V4_07 (SQ) for SQM; disambiguation DROPs; zero-arg wrapper; core logic preserved.
--   * Derived from prior SQM CANNON; core logic preserved exactly.
--   * Only change: destination tables are created if missing via wrapper (schema only), and wrappers follow V4_07 pattern.

BEGIN;

-- Clean out older overloads to avoid ambiguity
DROP FUNCTION IF EXISTS engine.build_sr_series_sqm() CASCADE;
DROP FUNCTION IF EXISTS engine.build_sr_series_sqm(uuid) CASCADE;
DROP FUNCTION IF EXISTS engine.build_sr_series_sqm(uuid, uuid) CASCADE;
DROP FUNCTION IF EXISTS engine.build_sr_series_sqm_core(uuid) CASCADE;
COMMIT;

BEGIN;
CREATE OR REPLACE FUNCTION engine.build_sr_series_sqm_core(p_forecast_id uuid DEFAULT NULL)
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

  RAISE NOTICE '[%] build_sr_series_sqm — start', clock_timestamp();

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
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_sqm';

    RAISE NOTICE '[%] SERIES % — target table engine.%', clock_timestamp(), mdl.model, out_tbl;

    
    -- UNIVERSAL SEGMENTING: per-forecast source + temp work mirror
    -- Source slice
    EXECUTE format('DROP TABLE IF EXISTS __universe'); 
    EXECUTE format('CREATE TEMP TABLE __universe AS SELECT h.* FROM engine.instance_historical h WHERE h.forecast_id = %L', fid);
    -- Work mirror of destination
    EXECUTE format('DROP TABLE IF EXISTS __work'); 
    EXECUTE format('CREATE TEMP TABLE __work (LIKE engine.%I INCLUDING ALL)', out_tbl);
    EXECUTE 'CREATE INDEX ON __work (forecast_id, date)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS __work_date_idx ON __work (date)';
IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
      RAISE EXCEPTION 'Target table engine.% does not exist. Create it first.', out_tbl;
    END IF;

    -- PASS 1: base insert with UPSERT
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 1 — insert/upsert into engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      INSERT INTO __work (
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
      FROM __universe h
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
      mdl.model||'_qm',              -- %2
      mdl.model||'_yqm',             -- %3
      mdl.model||'_smv',             -- %4
      mdl.model||'_qsr',             -- %5
      mdl.model||'_msr',             -- %6
      mdl.model||'_qm_p1_k',         -- %7
      mdl.model||'_qm_p2_k',         -- %8
      mdl.model||'_qm_p3_k',         -- %9
      fid,                           -- %10
      mdl.model                      -- %11
    );
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 1 done — rows affected: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- Covering index to accelerate lookbacks
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON engine.%I (%I, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_sqm_fid_yqm_date',
      out_tbl,
      'forecast_id', mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );
    -- Covering index on temp work table
    EXECUTE 'CREATE INDEX IF NOT EXISTS __work_cover_idx ON __work (forecast_id, date)';
    -- Analyze so planner uses fresh stats/index
    EXECUTE format('ANALYZE __work', out_tbl);

    -- PASS 2 legacy (kept, disabled)
    IF run_legacy_pass2 THEN
      _t_pass := clock_timestamp();
      RAISE NOTICE '[%] PASS 2 (legacy) — engine.%', _t_pass, out_tbl;

      EXECUTE format($q$
        UPDATE engine.%1$I AS t
        SET
          %2$I = (SELECT p.%6$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%5$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %3$I = (SELECT p.%7$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%5$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),

          %8$I = (SELECT p.%6$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%9$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %10$I= (SELECT p.%7$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%9$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),

          %11$I= (SELECT p.%6$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%12$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1),
          %13$I= (SELECT p.%7$I FROM __work p
                  WHERE p.forecast_id=t.forecast_id AND p.%4$I=t.%12$I AND p.date<t.date
                  ORDER BY p.date DESC LIMIT 1)
        WHERE t.forecast_id = %14$L;
      $q$,
        out_tbl,
        mdl.model||'_qm_p1_qsr', mdl.model||'_qm_p1_msr',
        mdl.model||'_yqm',       mdl.model||'_qm_p1_k',
        mdl.model||'_qsr',       mdl.model||'_msr',
        mdl.model||'_qm_p2_qsr', mdl.model||'_qm_p2_k', mdl.model||'_qm_p2_msr',
        mdl.model||'_qm_p3_qsr', mdl.model||'_qm_p3_k', mdl.model||'_qm_p3_msr',
        fid
      );

      GET DIAGNOSTICS _rows = ROW_COUNT;
      RAISE NOTICE '[%] PASS 2 (legacy) done — rows updated: %, elapsed: %.3f s',
        clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);
    END IF;

    -- PASS 2B optimized (set-based lookbacks via DISTINCT ON)
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 2B — lookbacks via DISTINCT ON — engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      WITH
      p1 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM __work t
        JOIN __work s
          ON s.forecast_id = t.forecast_id
         AND s.%4$I        = t.%5$I
         AND s.date        < t.date
        WHERE t.forecast_id = %14$L
        ORDER BY t.forecast_id, t.date, s.date DESC
      ),
      p2 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM __work t
        JOIN __work s
          ON s.forecast_id = t.forecast_id
         AND s.%4$I        = t.%9$I
         AND s.date        < t.date
        WHERE t.forecast_id = %14$L
        ORDER BY t.forecast_id, t.date, s.date DESC
      ),
      p3 AS (
        SELECT DISTINCT ON (t.forecast_id, t.date)
               t.forecast_id, t.date, s.%6$I AS qsr, s.%7$I AS msr
        FROM __work t
        JOIN __work s
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
      UPDATE __work t
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
      mdl.model||'_qm_p1_qsr', mdl.model||'_qm_p1_msr',
      mdl.model||'_yqm',       mdl.model||'_qm_p1_k',
      mdl.model||'_qsr',       mdl.model||'_msr',
      mdl.model||'_qm_p2_qsr', mdl.model||'_qm_p2_k', mdl.model||'_qm_p2_msr',
      mdl.model||'_qm_p3_qsr', mdl.model||'_qm_p3_k', mdl.model||'_qm_p3_msr',
      fid
    );
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 2B done — rows updated: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- ANALYZE after PASS 2B (added)
    EXECUTE format('ANALYZE __work', out_tbl);

    -- PASS 3: blends (unchanged)
    _t_pass := clock_timestamp();
    RAISE NOTICE '[%] PASS 3 — blends — engine.%', _t_pass, out_tbl;

    EXECUTE format($q$
      UPDATE __work t
      SET
        %1$s_qm_fqsr_a1  = ((%1$s_qm_p1_qsr + %1$s_qm_p2_qsr)/2.0),
        %1$s_qm_fqsr_a2  = ((%1$s_qm_p1_qsr * 0.75) + (%1$s_qm_p2_qsr * 0.25)),
        %1$s_qm_fqsr_a2w = ((%1$s_qm_p1_qsr * 0.75) + (%1$s_qm_p2_qsr * 0.25)),
        %1$s_qm_fqsr_a3  = ((%1$s_qm_p1_qsr + %1$s_qm_p2_qsr + %1$s_qm_p3_qsr)/3.0),
        %1$s_qm_fqsr_a3w = ((%1$s_qm_p1_qsr * 0.5) + (%1$s_qm_p2_qsr * 0.3) + (%1$s_qm_p3_qsr * 0.2)),

        %1$s_qm_fmsr_a1  = ((%1$s_qm_p1_msr + %1$s_qm_p2_msr)/2.0),
        %1$s_qm_fmsr_a2  = ((%1$s_qm_p1_msr * 0.75) + (%1$s_qm_p2_msr * 0.25)),
        %1$s_qm_fmsr_a2w = ((%1$s_qm_p1_msr * 0.75) + (%1$s_qm_p2_msr * 0.25)),
        %1$s_qm_fmsr_a3  = ((%1$s_qm_p1_msr + %1$s_qm_p2_msr + %1$s_qm_p3_msr)/3.0),
        %1$s_qm_fmsr_a3w = ((%1$s_qm_p1_msr * 0.5) + (%1$s_qm_p2_msr * 0.3) + (%1$s_qm_p3_msr * 0.2))
      WHERE t.forecast_id = %3$L;
    $q$, mdl.model, out_tbl, fid);
    GET DIAGNOSTICS _rows = ROW_COUNT;
    RAISE NOTICE '[%] PASS 3 done — rows updated: %, elapsed: %.3f s',
      clock_timestamp(), _rows, EXTRACT(epoch FROM clock_timestamp() - _t_pass);

    -- ANALYZE after PASS 3 (added)
    EXECUTE format('ANALYZE __work', out_tbl);

    -- FINAL MERGE: upsert temp results into destination
    EXECUTE format($m$
      INSERT INTO engine.%1$I AS dest
      SELECT * FROM __work
      ON CONFLICT (forecast_id, date) DO UPDATE SET
        value = EXCLUDED.value,
        qmv   = EXCLUDED.qmv,
        mmv   = EXCLUDED.mmv
      $m$, out_tbl);
    -- Merge the dynamic columns via column list (keeps math identical)
    PERFORM 1; -- placeholder (dynamic columns already included from SELECT *)
RAISE NOTICE '[%] SERIES % — complete', clock_timestamp(), mdl.model;
END LOOP;

  RAISE NOTICE '[%] build_sr_series_sqm — done (elapsed %.3f s)', clock_timestamp(), EXTRACT(epoch FROM clock_timestamp() - _t_start);
END;
$$;
COMMIT;

CREATE OR REPLACE FUNCTION engine.build_sr_series_sqm()
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  fid uuid;
  mdl record;
  out_tbl text;
  -- dynamic identifier variables
  c_qm   text;
  c_yqm  text;
  c_smv  text;
  c_qsr  text;
  c_msr  text;
  c_k1   text;
  c_k2   text;
  c_k3   text;
  c_p1_qsr text; c_p1_msr text;
  c_p2_qsr text; c_p2_msr text;
  c_p3_qsr text; c_p3_msr text;
  c_fqsr_a1 text; c_fqsr_a2 text; c_fqsr_a2w text; c_fqsr_a3 text; c_fqsr_a3w text;
  c_fmsr_a1 text; c_fmsr_a2 text; c_fmsr_a2w text; c_fmsr_a3 text; c_fmsr_a3w text;
  -- source types
  t_qm   text;
  t_yqm  text;
  t_k1   text;
  t_k2   text;
  t_k3   text;
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
  IF fid IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found in engine.instance_historical.';
  END IF;

  FOR mdl IN
    SELECT c.relname AS model
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='engine' AND c.relkind IN ('r','f')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name='date')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_sqm';

    -- build identifier names
    c_qm   := mdl.model||'_qm';
    c_yqm  := mdl.model||'_yqm';
    c_smv  := mdl.model||'_smv';
    c_qsr  := mdl.model||'_qsr';
    c_msr  := mdl.model||'_msr';
    c_k1   := mdl.model||'_qm_p1_k';
    c_k2   := mdl.model||'_qm_p2_k';
    c_k3   := mdl.model||'_qm_p3_k';

    c_p1_qsr := mdl.model||'_qm_p1_qsr';
    c_p1_msr := mdl.model||'_qm_p1_msr';
    c_p2_qsr := mdl.model||'_qm_p2_qsr';
    c_p2_msr := mdl.model||'_qm_p2_msr';
    c_p3_qsr := mdl.model||'_qm_p3_qsr';
    c_p3_msr := mdl.model||'_qm_p3_msr';

    c_fqsr_a1  := mdl.model||'_qm_fqsr_a1';
    c_fqsr_a2  := mdl.model||'_qm_fqsr_a2';
    c_fqsr_a2w := mdl.model||'_qm_fqsr_a2w';
    c_fqsr_a3  := mdl.model||'_qm_fqsr_a3';
    c_fqsr_a3w := mdl.model||'_qm_fqsr_a3w';

    c_fmsr_a1  := mdl.model||'_qm_fmsr_a1';
    c_fmsr_a2  := mdl.model||'_qm_fmsr_a2';
    c_fmsr_a2w := mdl.model||'_qm_fmsr_a2w';
    c_fmsr_a3  := mdl.model||'_qm_fmsr_a3';
    c_fmsr_a3w := mdl.model||'_qm_fmsr_a3w';

    -- source types from model table
    SELECT
      (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=c_qm)  AS t_qm,
      (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=c_yqm) AS t_yqm,
      (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=c_k1)  AS t_k1,
      (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=c_k2)  AS t_k2,
      (SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class cc ON cc.oid=a.attrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='engine' AND cc.relname=mdl.model AND a.attname=c_k3)  AS t_k3
    INTO t_qm, t_yqm, t_k1, t_k2, t_k3;

    -- create destination table if missing
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
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
        c_qm,  t_qm,
        c_yqm, t_yqm,
        c_smv,
        c_qsr,
        c_msr,
        c_k1,  t_k1,
        c_k2,  t_k2,
        c_k3,  t_k3,
        c_p1_qsr,
        c_p1_msr,
        c_p2_qsr,
        c_p2_msr,
        c_p3_qsr,
        c_p3_msr,
        c_fqsr_a1,
        c_fqsr_a2,
        c_fqsr_a2w,
        c_fqsr_a3,
        c_fqsr_a3w,
        c_fmsr_a1,
        c_fmsr_a2,
        c_fmsr_a2w,
        c_fmsr_a3,
        c_fmsr_a3w
      );

      -- grants
      EXECUTE format('GRANT SELECT ON engine.%I TO matrix_reader', out_tbl);
      EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO tsf_engine_app', out_tbl);
    END IF;
  END LOOP;

  -- Call the original function (contains all math/logic)
  PERFORM engine.build_sr_series_sqm_core(fid);
END;
$$;


GRANT EXECUTE ON FUNCTION engine.build_sr_series_sqm() TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.build_sr_series_sqm_core(uuid) TO matrix_reader, tsf_engine_app;