-- Binomial p-value lookup (two-sided at p = 0.5)
-- VC 1.4: Re-run safe in same session — pre-drop __log_* temps each loop; keep underflow guard & float8 math.
-- VC 1.3: Underflow-guarded builder — avoid exp() when ln(p) < -745; log-sum-exp throughout; idempotent.
-- VC 1.2: Float8-only math; windowed cumulative tails; per-n batching.
-- VC 1.1: Double-precision constants to avoid implicit NUMERIC casts.
-- VC 1.0: Initial builder (engine.binom_log, engine.binom_p).

DO $$
DECLARE
  max_n int := 2000;  -- set your ceiling; you can re-run later with a higher value to extend
  nn    int;
  ln2   double precision := ln(2.0::double precision);
  EXP_UNDERFLOW_CUTOFF constant double precision := -745.0;  -- ~ln(min float8)
BEGIN
  -- Idempotent tables
  CREATE TABLE IF NOT EXISTS engine.binom_log (
    n    int NOT NULL,
    i    int NOT NULL,
    lp_i double precision NOT NULL,       -- ln C(n,i)
    PRIMARY KEY (n,i)
  );

  CREATE TABLE IF NOT EXISTS engine.binom_p (
    n            int NOT NULL,
    k            int NOT NULL,
    p_two_sided  double precision NOT NULL,
    PRIMARY KEY (n,k)
  );

  -- Build per n
  FOR nn IN 0..max_n LOOP
    -- Ensure ln-combination rows for this n
    WITH i AS (
      SELECT gs AS i FROM generate_series(0, nn) AS gs
    ), missing AS (
      SELECT nn AS n, i.i
      FROM i
      LEFT JOIN engine.binom_log bl ON bl.n = nn AND bl.i = i.i
      WHERE bl.n IS NULL
    )
    INSERT INTO engine.binom_log (n, i, lp_i)
    SELECT m.n, m.i,
           COALESCE( (SELECT SUM( ln((m.n - j + 1)::double precision) - ln(j::double precision) )
                      FROM generate_series(1, m.i) AS j), 0.0::double precision)
    FROM missing m;

    -- **Re-run safety in same session**: drop any leftover temps from a prior aborted attempt
    DROP TABLE IF EXISTS __log_tail;
    DROP TABLE IF EXISTS __log_tail_m;
    DROP TABLE IF EXISTS __log_tail_cs;

    -- Build log probabilities logp_i = ln C(n,i) - n*ln 2
    CREATE TEMP TABLE __log_tail ON COMMIT DROP AS
    SELECT
      bl.i,
      (bl.lp_i - nn::double precision * ln2) AS logp
    FROM engine.binom_log bl
    WHERE bl.n = nn
    ORDER BY bl.i;

    -- Running maxima m_i for numerical stability
    CREATE TEMP TABLE __log_tail_m ON COMMIT DROP AS
    SELECT
      i,
      logp,
      MAX(logp) OVER (ORDER BY i ROWS UNBOUNDED PRECEDING) AS m_i
    FROM __log_tail
    ORDER BY i;

    -- log_csum(i) = m_i + ln( Σ_{j≤i} exp(logp_j - m_i) )
    CREATE TEMP TABLE __log_tail_cs ON COMMIT DROP AS
    SELECT
      i,
      m_i,
      ( m_i + ln( SUM( exp(logp - m_i) )
                  OVER (ORDER BY i ROWS UNBOUNDED PRECEDING) ) ) AS log_csum
    FROM __log_tail_m
    ORDER BY i;

    -- Upsert p-values for all k using lower tail at min(k, n-k)
    -- p_two_sided = min(1, 2 * exp(log_csum(min(k, n-k))))
    INSERT INTO engine.binom_p (n, k, p_two_sided)
    SELECT
      nn AS n,
      k.k,
      CASE
        WHEN (ln2 + cs.log_csum) <= EXP_UNDERFLOW_CUTOFF
          THEN 0.0::double precision
        ELSE LEAST(1.0::double precision, exp(ln2 + cs.log_csum))
      END AS p_two_sided
    FROM generate_series(0, nn) AS k(k)
    JOIN __log_tail_cs cs
      ON cs.i = LEAST(k.k, nn - k.k)
    ON CONFLICT (n,k) DO UPDATE
      SET p_two_sided = EXCLUDED.p_two_sided;

    -- Tidy (also protects next loop if user interrupts mid-run)
    DROP TABLE IF EXISTS __log_tail;
    DROP TABLE IF EXISTS __log_tail_m;
    DROP TABLE IF EXISTS __log_tail_cs;
  END LOOP;

  ANALYZE engine.binom_log;
  ANALYZE engine.binom_p;
END
$$ LANGUAGE plpgsql;
