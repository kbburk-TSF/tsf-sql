-- 000_Create_SR_Instance_tables_ALL_with_ownership.sql
-- VC 1.3 (2025-09-12): Create ALL SR instance tables (S, SQ, SQM) for every seasonal model,
--                      create covering indexes, grant DML, and align ownership with SR builder functions.
--                      Also flips builder/trigger functions to SECURITY DEFINER under the same owner.

DO $$
DECLARE
  owner_role   text := current_user;                 -- final owner of tables AND SR builder functions
  writer_roles text[] := ARRAY['public'];            -- roles granted DML on the tables

  mdl   record;
  s_tbl text;
  q_tbl text;
  qm_tbl text;
  r text;

  -- helper for safe ALTER FUNCTION (skip if function missing)
  proc_exists boolean;
BEGIN
  -- Ensure schema exists & is usable by writers
  EXECUTE 'CREATE SCHEMA IF NOT EXISTS engine';
  FOREACH r IN ARRAY writer_roles LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA engine TO %I', r);
  END LOOP;

  -- Discover EVERY seasonal model (same predicates as your SR builders)
  FOR mdl IN
    SELECT c.relname AS model
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'engine'
      AND c.relkind IN ('r','f')
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
    s_tbl  := mdl.model || '_instance_sr_s';
    q_tbl  := mdl.model || '_instance_sr_sq';
    qm_tbl := mdl.model || '_instance_sr_sqm';

    -- Recreate (drop if exists)
    EXECUTE format('DROP TABLE IF EXISTS engine.%I', s_tbl);
    EXECUTE format('DROP TABLE IF EXISTS engine.%I', q_tbl);
    EXECUTE format('DROP TABLE IF EXISTS engine.%I', qm_tbl);

    -- =========================
    -- S SERIES
    -- =========================
    EXECUTE format($q$
      CREATE TABLE engine.%1$I (
        forecast_id       uuid NOT NULL,
        date              date NOT NULL,
        value             double precision,
        qmv               double precision,
        mmv               double precision,
        %2$I              text,
        %3$I              text,
        %4$I              double precision,
        %5$I              double precision,
        %6$I              double precision,
        %7$I              text,
        %8$I              double precision,
        %9$I              double precision,
        %10$I             text,
        %11$I             double precision,
        %12$I             double precision,
        %13$I             text,
        %14$I             double precision,
        %15$I             double precision,
        %16$I             double precision,
        %17$I             double precision,
        %18$I             double precision,
        %19$I             double precision,
        %20$I             double precision,
        %21$I             double precision,
        %22$I             double precision,
        %23$I             double precision,
        %24$I             double precision,
        %25$I             double precision,
        created_at        timestamptz DEFAULT now(),
        PRIMARY KEY (forecast_id, date)
      )
    $q$,
      s_tbl,
      mdl.model,                  -- %2  <model>
      mdl.model||'_yqm',          -- %3
      mdl.model||'_smv',          -- %4
      mdl.model||'_qsr',          -- %5
      mdl.model||'_msr',          -- %6
      mdl.model||'_p1_k',         -- %7
      mdl.model||'_p1_qsr',       -- %8
      mdl.model||'_p1_msr',       -- %9
      mdl.model||'_p2_k',         -- %10
      mdl.model||'_p2_qsr',       -- %11
      mdl.model||'_p2_msr',       -- %12
      mdl.model||'_p3_k',         -- %13
      mdl.model||'_p3_qsr',       -- %14
      mdl.model||'_p3_msr',       -- %15
      mdl.model||'_fqsr_a1',      -- %16
      mdl.model||'_fqsr_a2',      -- %17
      mdl.model||'_fqsr_a2w',     -- %18
      mdl.model||'_fqsr_a3',      -- %19
      mdl.model||'_fqsr_a3w',     -- %20
      mdl.model||'_fmsr_a1',      -- %21
      mdl.model||'_fmsr_a2',      -- %22
      mdl.model||'_fmsr_a2w',     -- %23
      mdl.model||'_fmsr_a3',      -- %24
      mdl.model||'_fmsr_a3w'      -- %25
    );

    -- covering index (the builder functions expect this shape)
    EXECUTE format(
      'CREATE INDEX %I ON engine.%I (forecast_id, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_s_fid_yqm_date',
      s_tbl, mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );

    FOREACH r IN ARRAY writer_roles LOOP
      EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRIGGER, REFERENCES ON engine.%I TO %I', s_tbl, r);
    END LOOP;
    EXECUTE format('ALTER TABLE engine.%I OWNER TO %I', s_tbl, owner_role);

    -- =========================
    -- SQ SERIES
    -- =========================
    EXECUTE format($q$
      CREATE TABLE engine.%1$I (
        forecast_id         uuid NOT NULL,
        date                date NOT NULL,
        value               double precision,
        qmv                 double precision,
        mmv                 double precision,
        %2$I                text,
        %3$I                text,
        %4$I                double precision,
        %5$I                double precision,
        %6$I                double precision,
        %7$I                text,
        %8$I                double precision,
        %9$I                double precision,
        %10$I               text,
        %11$I               double precision,
        %12$I               double precision,
        %13$I               text,
        %14$I               double precision,
        %15$I               double precision,
        %16$I               double precision,
        %17$I               double precision,
        %18$I               double precision,
        %19$I               double precision,
        %20$I               double precision,
        %21$I               double precision,
        %22$I               double precision,
        %23$I               double precision,
        %24$I               double precision,
        %25$I               double precision,
        created_at          timestamptz DEFAULT now(),
        PRIMARY KEY (forecast_id, date)
      )
    $q$,
      q_tbl,
      mdl.model||'_q',            -- %2
      mdl.model||'_yqm',          -- %3
      mdl.model||'_smv',          -- %4
      mdl.model||'_qsr',          -- %5
      mdl.model||'_msr',          -- %6
      mdl.model||'_q_p1_k',       -- %7
      mdl.model||'_q_p1_qsr',     -- %8
      mdl.model||'_q_p1_msr',     -- %9
      mdl.model||'_q_p2_k',       -- %10
      mdl.model||'_q_p2_qsr',     -- %11
      mdl.model||'_q_p2_msr',     -- %12
      mdl.model||'_q_p3_k',       -- %13
      mdl.model||'_q_p3_qsr',     -- %14
      mdl.model||'_q_p3_msr',     -- %15
      mdl.model||'_q_fqsr_a1',    -- %16
      mdl.model||'_q_fqsr_a2',    -- %17
      mdl.model||'_q_fqsr_a2w',   -- %18
      mdl.model||'_q_fqsr_a3',    -- %19
      mdl.model||'_q_fqsr_a3w',   -- %20
      mdl.model||'_q_fmsr_a1',    -- %21
      mdl.model||'_q_fmsr_a2',    -- %22
      mdl.model||'_q_fmsr_a2w',   -- %23
      mdl.model||'_q_fmsr_a3',    -- %24
      mdl.model||'_q_fmsr_a3w'    -- %25
    );

    EXECUTE format(
      'CREATE INDEX %I ON engine.%I (forecast_id, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_sq_fid_yqm_date',
      q_tbl, mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );

    FOREACH r IN ARRAY writer_roles LOOP
      EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRIGGER, REFERENCES ON engine.%I TO %I', q_tbl, r);
    END LOOP;
    EXECUTE format('ALTER TABLE engine.%I OWNER TO %I', q_tbl, owner_role);

    -- =========================
    -- SQM SERIES
    -- =========================
    EXECUTE format($q$
      CREATE TABLE engine.%1$I (
        forecast_id         uuid NOT NULL,
        date                date NOT NULL,
        value               double precision,
        qmv                 double precision,
        mmv                 double precision,
        %2$I                text,
        %3$I                text,
        %4$I                double precision,
        %5$I                double precision,
        %6$I                double precision,
        %7$I                text,
        %8$I                double precision,
        %9$I                double precision,
        %10$I               text,
        %11$I               double precision,
        %12$I               double precision,
        %13$I               text,
        %14$I               double precision,
        %15$I               double precision,
        %16$I               double precision,
        %17$I               double precision,
        %18$I               double precision,
        %19$I               double precision,
        %20$I               double precision,
        %21$I               double precision,
        %22$I               double precision,
        %23$I               double precision,
        %24$I               double precision,
        %25$I               double precision,
        created_at          timestamptz DEFAULT now(),
        PRIMARY KEY (forecast_id, date)
      )
    $q$,
      qm_tbl,
      mdl.model||'_qm',           -- %2
      mdl.model||'_yqm',          -- %3
      mdl.model||'_smv',          -- %4
      mdl.model||'_qsr',          -- %5
      mdl.model||'_msr',          -- %6
      mdl.model||'_qm_p1_k',      -- %7
      mdl.model||'_qm_p1_qsr',    -- %8
      mdl.model||'_qm_p1_msr',    -- %9
      mdl.model||'_qm_p2_k',      -- %10
      mdl.model||'_qm_p2_qsr',    -- %11
      mdl.model||'_qm_p2_msr',    -- %12
      mdl.model||'_qm_p3_k',      -- %13
      mdl.model||'_qm_p3_qsr',    -- %14
      mdl.model||'_qm_p3_msr',    -- %15
      mdl.model||'_qm_fqsr_a1',   -- %16
      mdl.model||'_qm_fqsr_a2',   -- %17
      mdl.model||'_qm_fqsr_a2w',  -- %18
      mdl.model||'_qm_fqsr_a3',   -- %19
      mdl.model||'_qm_fqsr_a3w',  -- %20
      mdl.model||'_qm_fmsr_a1',   -- %21
      mdl.model||'_qm_fmsr_a2',   -- %22
      mdl.model||'_qm_fmsr_a2w',  -- %23
      mdl.model||'_qm_fmsr_a3',   -- %24
      mdl.model||'_qm_fmsr_a3w'   -- %25
    );

    EXECUTE format(
      'CREATE INDEX %I ON engine.%I (forecast_id, %I, date) INCLUDE (%I, %I)',
      'ix_'||mdl.model||'_sr_sqm_fid_yqm_date',
      qm_tbl, mdl.model||'_yqm', mdl.model||'_qsr', mdl.model||'_msr'
    );

    FOREACH r IN ARRAY writer_roles LOOP
      EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRIGGER, REFERENCES ON engine.%I TO %I', qm_tbl, r);
    END LOOP;
    EXECUTE format('ALTER TABLE engine.%I OWNER TO %I', qm_tbl, owner_role);
  END LOOP;

  -- Align SR builder functions & trigger function with the same owner and SECURITY DEFINER
  -- (this lets them create indexes when invoked by other roles)
  SELECT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='engine' AND p.proname='build_sr_series_s' AND pg_get_function_identity_arguments(p.oid)='uuid'
  ) INTO proc_exists;
  IF proc_exists THEN
    EXECUTE format('ALTER FUNCTION engine.build_sr_series_s(uuid) OWNER TO %I', owner_role);
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_s(uuid) SECURITY DEFINER';
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_s(uuid) SET search_path = engine, pg_temp';
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='engine' AND p.proname='build_sr_series_sq' AND pg_get_function_identity_arguments(p.oid)='uuid'
  ) INTO proc_exists;
  IF proc_exists THEN
    EXECUTE format('ALTER FUNCTION engine.build_sr_series_sq(uuid) OWNER TO %I', owner_role);
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_sq(uuid) SECURITY DEFINER';
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_sq(uuid) SET search_path = engine, pg_temp';
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='engine' AND p.proname='build_sr_series_sqm' AND pg_get_function_identity_arguments(p.oid)='uuid'
  ) INTO proc_exists;
  IF proc_exists THEN
    EXECUTE format('ALTER FUNCTION engine.build_sr_series_sqm(uuid) OWNER TO %I', owner_role);
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_sqm(uuid) SECURITY DEFINER';
    EXECUTE 'ALTER FUNCTION engine.build_sr_series_sqm(uuid) SET search_path = engine, pg_temp';
  END IF;

  -- Your trigger function that fires the builder (name inferred from error)
  SELECT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='engine' AND p.proname='trg_finalize_hist_commit' AND pg_get_function_identity_arguments(p.oid)=''
  ) INTO proc_exists;
  IF proc_exists THEN
    EXECUTE format('ALTER FUNCTION engine.trg_finalize_hist_commit() OWNER TO %I', owner_role);
    EXECUTE 'ALTER FUNCTION engine.trg_finalize_hist_commit() SECURITY DEFINER';
    EXECUTE 'ALTER FUNCTION engine.trg_finalize_hist_commit() SET search_path = engine, pg_temp';
  END IF;

  -- Let writers call the functions
  FOREACH r IN ARRAY writer_roles LOOP
    PERFORM 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='engine' AND p.proname='build_sr_series_s' AND pg_get_function_identity_arguments(p.oid)='uuid';
    IF FOUND THEN EXECUTE format('GRANT EXECUTE ON FUNCTION engine.build_sr_series_s(uuid) TO %I', r); END IF;

    PERFORM 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='engine' AND p.proname='build_sr_series_sq' AND pg_get_function_identity_arguments(p.oid)='uuid';
    IF FOUND THEN EXECUTE format('GRANT EXECUTE ON FUNCTION engine.build_sr_series_sq(uuid) TO %I', r); END IF;

    PERFORM 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='engine' AND p.proname='build_sr_series_sqm' AND pg_get_function_identity_arguments(p.oid)='uuid';
    IF FOUND THEN EXECUTE format('GRANT EXECUTE ON FUNCTION engine.build_sr_series_sqm(uuid) TO %I', r); END IF;

    PERFORM 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='engine' AND p.proname='trg_finalize_hist_commit' AND pg_get_function_identity_arguments(p.oid)='';
    IF FOUND THEN EXECUTE format('GRANT EXECUTE ON FUNCTION engine.trg_finalize_hist_commit() TO %I', r); END IF;
  END LOOP;

END $$;
