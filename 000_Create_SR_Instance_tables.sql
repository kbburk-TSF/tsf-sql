-- SR_SERIES_INSTANCE_TABLES_with_grants.sql
-- Creates EMPTY SR tables for one seasonal model AND immediately grants write access.
-- Set the model (base table name) here, e.g. 'me_s_mr30'
DO $$
DECLARE
  model  text := 'me_s_mr30'; -- <model>
  s_tbl  text := model || '_instance_sr_s';
  q_tbl  text := model || '_instance_sr_sq';
  qm_tbl text := model || '_instance_sr_sqm';
BEGIN
  -- Recreate empty tables
  EXECUTE format('DROP TABLE IF EXISTS engine.%I;', s_tbl);
  EXECUTE format('DROP TABLE IF EXISTS engine.%I;', q_tbl);
  EXECUTE format('DROP TABLE IF EXISTS engine.%I;', qm_tbl);

  ---------------------------------------------------------------------------
  -- S SERIES: engine.<model>_instance_sr_s
  ---------------------------------------------------------------------------
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
    );
  $q$,
    s_tbl,
    model,                  -- %2  <model>
    model||'_yqm',          -- %3
    model||'_smv',          -- %4
    model||'_qsr',          -- %5
    model||'_msr',          -- %6
    model||'_p1_k',         -- %7
    model||'_p1_qsr',       -- %8
    model||'_p1_msr',       -- %9
    model||'_p2_k',         -- %10
    model||'_p2_qsr',       -- %11
    model||'_p2_msr',       -- %12
    model||'_p3_k',         -- %13
    model||'_p3_qsr',       -- %14
    model||'_p3_msr',       -- %15
    model||'_fqsr_a1',      -- %16
    model||'_fqsr_a2',      -- %17
    model||'_fqsr_a2w',     -- %18
    model||'_fqsr_a3',      -- %19
    model||'_fqsr_a3w',     -- %20
    model||'_fmsr_a1',      -- %21
    model||'_fmsr_a2',      -- %22
    model||'_fmsr_a2w',     -- %23
    model||'_fmsr_a3',      -- %24
    model||'_fmsr_a3w'      -- %25
  );

  -- Grant write access immediately (so pipeline can INSERT/UPDATE)
  EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO PUBLIC;', s_tbl);

  ---------------------------------------------------------------------------
  -- Q SERIES: engine.<model>_instance_sr_sq
  ---------------------------------------------------------------------------
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
    );
  $q$,
    q_tbl,
    model||'_q',            -- %2
    model||'_yqm',          -- %3
    model||'_smv',          -- %4
    model||'_qsr',          -- %5
    model||'_msr',          -- %6
    model||'_q_p1_k',       -- %7
    model||'_q_p1_qsr',     -- %8
    model||'_q_p1_msr',     -- %9
    model||'_q_p2_k',       -- %10
    model||'_q_p2_qsr',     -- %11
    model||'_q_p2_msr',     -- %12
    model||'_q_p3_k',       -- %13
    model||'_q_p3_qsr',     -- %14
    model||'_q_p3_msr',     -- %15
    model||'_q_fqsr_a1',    -- %16
    model||'_q_fqsr_a2',    -- %17
    model||'_q_fqsr_a2w',   -- %18
    model||'_q_fqsr_a3',    -- %19
    model||'_q_fqsr_a3w',   -- %20
    model||'_q_fmsr_a1',    -- %21
    model||'_q_fmsr_a2',    -- %22
    model||'_q_fmsr_a2w',   -- %23
    model||'_q_fmsr_a3',    -- %24
    model||'_q_fmsr_a3w'    -- %25
  );

  EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO PUBLIC;', q_tbl);

  ---------------------------------------------------------------------------
  -- QM SERIES: engine.<model>_instance_sr_sqm
  ---------------------------------------------------------------------------
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
    );
  $q$,
    qm_tbl,
    model||'_qm',           -- %2
    model||'_yqm',          -- %3
    model||'_smv',          -- %4
    model||'_qsr',          -- %5
    model||'_msr',          -- %6
    model||'_qm_p1_k',      -- %7
    model||'_qm_p1_qsr',    -- %8
    model||'_qm_p1_msr',    -- %9
    model||'_qm_p2_k',      -- %10
    model||'_qm_p2_qsr',    -- %11
    model||'_qm_p2_msr',    -- %12
    model||'_qm_p3_k',      -- %13
    model||'_qm_p3_qsr',    -- %14
    model||'_qm_p3_msr',    -- %15
    model||'_qm_fqsr_a1',   -- %16
    model||'_qm_fqsr_a2',   -- %17
    model||'_qm_fqsr_a2w',  -- %18
    model||'_qm_fqsr_a3',   -- %19
    model||'_qm_fqsr_a3w',  -- %20
    model||'_qm_fmsr_a1',   -- %21
    model||'_qm_fmsr_a2',   -- %22
    model||'_qm_fmsr_a2w',  -- %23
    model||'_qm_fmsr_a3',   -- %24
    model||'_qm_fmsr_a3w'   -- %25
  );

  EXECUTE format('GRANT SELECT, INSERT, UPDATE ON engine.%I TO PUBLIC;', qm_tbl);
END $$;
