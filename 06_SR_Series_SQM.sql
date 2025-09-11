-- FUNCTION: engine.build_sr_series_sqm(uuid) — SR Series SQM (quarter-month) builder
-- - Does NOT create/alter destination tables; raises if missing
-- - Uses the passed forecast_id; if NULL, falls back to latest by created_at
-- UPDATE 2025-09-11: add *_qm_fqsr_a0 and *_qm_fmsr_a0 (=1.0), align format() placeholders, and FIX SOURCE to read *_qm columns from engine.<model> (not engine.<model>_qm).
BEGIN;
CREATE OR REPLACE FUNCTION engine.build_sr_series_sqm(p_forecast_id uuid DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  fid     uuid;
  mdl     record;
  out_tbl text;
BEGIN
  -- Resolve forecast_id (latest by created_at if not provided)
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

  -- Iterate seasonal model BASE tables exposing *_qm columns
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
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_yqm')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p1_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p2_k')
      AND EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='engine' AND table_name=c.relname AND column_name=c.relname||'_qm_p3_k')
    ORDER BY c.relname
  LOOP
    out_tbl := mdl.model || '_instance_sr_sqm';

    -- Destination table must already exist
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='engine' AND table_name=out_tbl
    ) THEN
      RAISE EXCEPTION 'Target table engine.% does not exist. Create it first.', out_tbl;
    END IF;

    ---------------------------------------------------------------------------
    -- PASS 1: Base load (idempotent upsert) — SOURCE: engine.<model> (join to historical)
    ---------------------------------------------------------------------------
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
        m.%2$I,                -- <s_qm>
        m.%3$I,                -- <s_qm_yqm>
        avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I)                              AS %4$I, -- <s_qm_smv>
        (avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I) / NULLIF(h.qmv,0))           AS %5$I, -- <s_qm_qsr>
        (avg(h.value) FILTER (WHERE h.value IS NOT NULL)
          OVER (PARTITION BY m.%3$I) / NULLIF(h.mmv,0))           AS %6$I, -- <s_qm_msr>
        m.%7$I, m.%8$I, m.%9$I                                    -- qm_p1_k, qm_p2_k, qm_p3_k
      FROM engine.instance_historical h
      JOIN engine.%11$I m ON m.date = h.date
      WHERE h.forecast_id = %10$L
      ORDER BY h.date
      ON CONFLICT (forecast_id, date) DO UPDATE
      SET
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
      out_tbl,                       -- %1
      mdl.model||'_qm',              -- %2  <s_qm>
      mdl.model||'_qm_yqm',          -- %3  <s_qm_yqm>
      mdl.model||'_qm_smv',          -- %4  <s_qm_smv>
      mdl.model||'_qm_qsr',          -- %5  <s_qm_qsr>
      mdl.model||'_qm_msr',          -- %6  <s_qm_msr>
      mdl.model||'_qm_p1_k',         -- %7
      mdl.model||'_qm_p2_k',         -- %8
      mdl.model||'_qm_p3_k',         -- %9
      fid,                           -- %10
      mdl.model                      -- %11  SOURCE base table name
    );

    ---------------------------------------------------------------------------
    -- PASS 2: Lookbacks (populate qm_p1/qm_p2/qm_p3 qsr/msr from prior matching keys)
    ---------------------------------------------------------------------------
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
      mdl.model||'_qm_p1_qsr',     -- %2
      mdl.model||'_qm_p1_msr',     -- %3
      mdl.model||'_qm_yqm',        -- %4
      mdl.model||'_qm_p1_k',       -- %5
      mdl.model||'_qm_qsr',        -- %6
      mdl.model||'_qm_msr',        -- %7
      mdl.model||'_qm_p2_qsr',     -- %8
      mdl.model||'_qm_p2_k',       -- %9
      mdl.model||'_qm_p2_msr',     -- %10
      mdl.model||'_qm_p3_qsr',     -- %11
      mdl.model||'_qm_p3_k',       -- %12
      mdl.model||'_qm_p3_msr',     -- %13
      fid                          -- %14
    );

    ---------------------------------------------------------------------------
    -- PASS 3: Blends (+ new a0 constants=1.0)
    ---------------------------------------------------------------------------
    EXECUTE format($q$
      UPDATE engine.%2$I t
      SET
        %1$s_qm_fqsr_a0 = 1.0,
        %1$s_qm_fmsr_a0 = 1.0,

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
  END LOOP;
END;
$$;
COMMIT;
