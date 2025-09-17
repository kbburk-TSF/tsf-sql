-- V4_02_Binomial_Lookup_Local.sql
-- Local binomial p-value lookup (engine.binom_p), no FDW.
-- VC V4.0 (2025-09-17): Migrated from BINOMIAL_LOOKUP_SEASONAL_MODEL_DB.sql unchanged except for schema = engine.
--                   Drops & recreates engine.binom_p and repopulates via recurrence at p=0.5.
--                   Grants SELECT to matrix_reader and tsf_engine_app.

-- Ensure target schema exists (idempotent)
CREATE SCHEMA IF NOT EXISTS engine;

-- Binomial p-value lookup (two-sided, p = 0.5) in PUBLIC schema
-- VC 2.3 (public): rebuild table; p_two_sided stored as numeric(12,4);
--                  values that would round to 0.0000 are stored as 0.0001;
--                  recurrence method (no exp/ln), re-runnable.

DO $$
DECLARE
  max_n int := 1000;   -- adjust if ever needed; you can re-run later with a higher value to extend
  nn    int;
  half  int;
  p0    double precision;
  p     double precision;
  cs    double precision;
  i     int;
BEGIN
  -- Recreate lookup table with fixed 4-decimal scale
  DROP TABLE IF EXISTS engine.binom_p;

  CREATE TABLE engine.binom_p (
    n            int NOT NULL,
    k            int NOT NULL,
    p_two_sided  numeric(12,4) NOT NULL,
    PRIMARY KEY (n, k)
  );

  -- Build per n so temp state stays small; recurrence avoids underflow entirely
  FOR nn IN 0..max_n LOOP
    -- clean up if a prior attempt in this session left temps behind
    DROP TABLE IF EXISTS __cs;

    -- lower-tail probabilities via stable recurrence (no exp/ln):
    -- p(0)   = (1/2)^n
    -- p(i+1) = p(i) * ((n - i) / (i + 1)) * 1/2
    -- cs(i)  = Σ_{j=0..i} p(j)
    half := nn / 2;
    p0   := power(0.5::double precision, nn);
    p    := p0;
    cs   := p0;

    CREATE TEMP TABLE __cs (
      i  int PRIMARY KEY,
      cs double precision
    ) ON COMMIT DROP;

    INSERT INTO __cs(i, cs) VALUES (0, cs);

    FOR i IN 1..half LOOP
      p  := p * ((nn - (i - 1))::double precision / i::double precision) * 0.5::double precision;
      cs := cs + p;
      INSERT INTO __cs(i, cs) VALUES (i, cs);
    END LOOP;

    -- two-sided p(k; n, 0.5) = 2 * lower_tail(min(k, n-k)); cap at 1
    -- store as numeric(12,4); if it would round to 0.0000, store 0.0001 instead
    INSERT INTO engine.binom_p (n, k, p_two_sided)
    SELECT
      nn AS n,
      k.k,
      CASE
        WHEN round(LEAST(1.0::double precision, 2.0::double precision * c.cs)::numeric, 4) = 0::numeric
          THEN 0.0001::numeric(12,4)
        ELSE round(LEAST(1.0::double precision, 2.0::double precision * c.cs)::numeric, 4)::numeric(12,4)
      END AS p_two_sided
    FROM generate_series(0, nn) AS k(k)
    JOIN __cs c
      ON c.i = LEAST(k.k, nn - k.k);

    DROP TABLE IF EXISTS __cs;
  END LOOP;

  ANALYZE engine.binom_p;

  -- Make it readable by FDW/TablePlus users without extra grants
  GRANT SELECT ON engine.binom_p TO PUBLIC;
END
$$ LANGUAGE plpgsql;

-- Visibility & access
GRANT SELECT ON TABLE engine.binom_p TO matrix_reader, tsf_engine_app;
