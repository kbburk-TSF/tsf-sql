-- =====================================================================
-- Version: 2025-09-11  v1.7 (validate_historical_table build)
-- Purpose: Create engine.validate_historical_table from engine.staging_historical_sandbox
-- Implements qmv/mmv (look-forward) and LQM/LMM (look-back) per spec.
-- =====================================================================

DROP TABLE IF EXISTS engine.validate_historical_table;

CREATE TABLE engine.validate_historical_table AS
WITH s AS (
  SELECT
    shs.forecast_id,
    shs."DATE"::date                AS d,
    shs."VALUE"::double precision   AS v,
    shs."ARIMA-Q"::double precision AS arima_q,
    shs."SES-Q"::double precision   AS ses_q,
    shs."HWES-Q"::double precision  AS hwes_q,
    shs."ARIMA-M"::double precision AS arima_m,
    shs."SES-M"::double precision   AS ses_m,
    shs."HWES-M"::double precision  AS hwes_m
  FROM engine.staging_historical_sandbox shs
),
anchors AS (
  SELECT
    s.*,
    date_trunc('quarter', s.d)::date AS qcur,
    date_trunc('month',   s.d)::date AS mcur
  FROM s
),
bounds AS (
  SELECT
    forecast_id,
    min(d) FILTER (WHERE v IS NOT NULL) AS first_value_date,
    date_trunc('quarter', min(d) FILTER (WHERE v IS NOT NULL))::date AS first_quarter_start,
    date_trunc('month',   min(d) FILTER (WHERE v IS NOT NULL))::date AS first_month_start,
    (date_trunc('month', max(d) FILTER (WHERE v IS NOT NULL)) + interval '2 months' - interval '1 day')::date AS lmm_allowed_end
  FROM anchors
  GROUP BY forecast_id
),
qmv_mmv AS (
  SELECT
    a.*,
    avg(a.v) FILTER (WHERE a.v IS NOT NULL)
      OVER (PARTITION BY a.forecast_id, a.qcur) AS qmv_all,
    avg(a.v) FILTER (WHERE a.v IS NOT NULL)
      OVER (PARTITION BY a.forecast_id, a.mcur) AS mmv_all
  FROM anchors a
),
prev_quarter_ranked AS (
  SELECT
    forecast_id,
    date_trunc('quarter', d)::date AS qprev,
    v,
    row_number() OVER (PARTITION BY forecast_id, date_trunc('quarter', d) ORDER BY d DESC) AS rn
  FROM s
  WHERE v IS NOT NULL
),
lqm_by_qprev AS (
  SELECT
    forecast_id,
    qprev,
    max(v)  FILTER (WHERE rn = 1)   AS lqm1,
    avg(v)  FILTER (WHERE rn <= 5)  AS lqm5,
    avg(v)  FILTER (WHERE rn <= 10) AS lqm10,
    avg(v)  FILTER (WHERE rn <= 15) AS lqm15,
    avg(v)  FILTER (WHERE rn <= 30) AS lqm30
  FROM prev_quarter_ranked
  GROUP BY forecast_id, qprev
),
prev_month_ranked AS (
  SELECT
    forecast_id,
    date_trunc('month', d)::date AS mprev,
    v,
    row_number() OVER (PARTITION BY forecast_id, date_trunc('month', d) ORDER BY d DESC) AS rn
  FROM s
  WHERE v IS NOT NULL
),
lmm_by_mprev AS (
  SELECT
    forecast_id,
    mprev,
    max(v)  FILTER (WHERE rn = 1)   AS lmm1,
    avg(v)  FILTER (WHERE rn <= 5)  AS lmm5,
    avg(v)  FILTER (WHERE rn <= 10) AS lmm10,
    avg(v)  FILTER (WHERE rn <= 15) AS lmm15,
    avg(v)                          AS lmm30
  FROM prev_month_ranked
  GROUP BY forecast_id, mprev
)
SELECT
  a.forecast_id,
  a.d AS "date",
  a.v AS value,
  CASE WHEN a.v IS NULL THEN NULL ELSE q.qmv_all END AS qmv,
  CASE WHEN a.v IS NULL THEN NULL ELSE q.mmv_all END AS mmv,
  CASE WHEN a.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm1  END AS lqm1,
  CASE WHEN a.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm5  END AS lqm5,
  CASE WHEN a.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm10 END AS lqm10,
  CASE WHEN a.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm15 END AS lqm15,
  CASE WHEN a.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm30 END AS lqm30,
  CASE WHEN a.mcur = b.first_month_start OR a.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm1  END AS lmm1,
  CASE WHEN a.mcur = b.first_month_start OR a.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm5  END AS lmm5,
  CASE WHEN a.mcur = b.first_month_start OR a.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm10 END AS lmm10,
  CASE WHEN a.mcur = b.first_month_start OR a.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm15 END AS lmm15,
  CASE WHEN a.mcur = b.first_month_start OR a.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm30 END AS lmm30,
  a.arima_q, a.ses_q, a.hwes_q,
  a.arima_m, a.ses_m, a.hwes_m,
  now() AS created_at
FROM qmv_mmv q
JOIN anchors a
  ON a.forecast_id = q.forecast_id AND a.d = q.d
JOIN bounds b
  ON b.forecast_id = a.forecast_id
LEFT JOIN lqm_by_qprev lq
  ON lq.forecast_id = a.forecast_id
 AND lq.qprev = (a.qcur - interval '3 months')::date
LEFT JOIN lmm_by_mprev lm
  ON lm.forecast_id = a.forecast_id
 AND lm.mprev = (a.mcur - interval '1 month')::date
ORDER BY a.forecast_id, a.d;
