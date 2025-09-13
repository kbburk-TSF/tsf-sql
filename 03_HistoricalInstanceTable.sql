-- =====================================================================
-- Version: 2025-09-11  v2.1
-- Change: Convert 01_HistoricalInstanceTable.sql into TWO-PASS function:
--   PASS 1 = hydrate raw rows from staging; PASS 2 = compute qmv/mmv & LQM/LMM.
--   No date generation. Preserves created_at. Writes to engine.instance_historical.
-- =====================================================================

CREATE OR REPLACE FUNCTION engine.build_instance_historical(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  /* ---------------- PASS 1: HYDRATE RAW ROWS (from CSV-loaded staging) ---------------- */
  INSERT INTO engine.instance_historical (
    forecast_id, "date", value, qmv, mmv,
    lqm1, lqm5, lqm10, lqm15, lqm30,
    lmm1, lmm5, lmm10, lmm15, lmm30,
    arima_q, ses_q, hwes_q, arima_m, ses_m, hwes_m, created_at
  )
  SELECT
    sh.forecast_id,
    sh."DATE"::date,
    sh."VALUE"::double precision,
    NULL::double precision, NULL::double precision,
    NULL::double precision, NULL::double precision, NULL::double precision, NULL::double precision, NULL::double precision,
    NULL::double precision, NULL::double precision, NULL::double precision, NULL::double precision, NULL::double precision,
    sh."ARIMA-Q"::double precision, sh."SES-Q"::double precision, sh."HWES-Q"::double precision,
    sh."ARIMA-M"::double precision, sh."SES-M"::double precision, sh."HWES-M"::double precision,
    now()
  FROM engine.staging_historical sh
  WHERE sh.forecast_id = p_forecast_id
  ON CONFLICT (forecast_id, "date") DO UPDATE SET
    value    = EXCLUDED.value,
    arima_q  = EXCLUDED.arima_q,  ses_q = EXCLUDED.ses_q,  hwes_q = EXCLUDED.hwes_q,
    arima_m  = EXCLUDED.arima_m,  ses_m = EXCLUDED.ses_m,  hwes_m = EXCLUDED.hwes_m,
    /* reset computed fields before recompute */
    qmv = NULL, mmv = NULL,
    lqm1 = NULL, lqm5 = NULL, lqm10 = NULL, lqm15 = NULL, lqm30 = NULL,
    lmm1 = NULL, lmm5 = NULL, lmm10 = NULL, lmm15 = NULL, lmm30 = NULL,
    created_at = COALESCE(engine.instance_historical.created_at, EXCLUDED.created_at);

  /* ---------------- PASS 2: COMPUTE qmv/mmv (look-forward) & LQM/LMM (look-back) ---------------- */
  WITH rows AS (
    SELECT
      ih.forecast_id,
      ih."date" AS d,
      ih.value,
      date_trunc('quarter', ih."date")::date AS qcur,
      date_trunc('month',   ih."date")::date AS mcur
    FROM engine.instance_historical ih
    WHERE ih.forecast_id = p_forecast_id
  ),
  bounds AS (
    SELECT
      r.forecast_id,
      min(r.d) FILTER (WHERE r.value IS NOT NULL) AS first_value_date,
      date_trunc('quarter', min(r.d) FILTER (WHERE r.value IS NOT NULL))::date AS first_quarter_start,
      date_trunc('month',   min(r.d) FILTER (WHERE r.value IS NOT NULL))::date AS first_month_start,
      (date_trunc('month', max(r.d) FILTER (WHERE r.value IS NOT NULL)) + interval '2 months' - interval '1 day')::date AS lmm_allowed_end
    FROM rows r
    GROUP BY r.forecast_id
  ),
  qagg AS (
    SELECT r.forecast_id, r.qcur, avg(r.value) FILTER (WHERE r.value IS NOT NULL) AS qmv_all
    FROM rows r GROUP BY 1,2
  ),
  magg AS (
    SELECT r.forecast_id, r.mcur, avg(r.value) FILTER (WHERE r.value IS NOT NULL) AS mmv_all
    FROM rows r GROUP BY 1,2
  ),
  q_rank AS (
    SELECT
      ih.forecast_id,
      date_trunc('quarter', ih."date")::date AS qprev,
      ih.value AS v,
      row_number() OVER (PARTITION BY ih.forecast_id, date_trunc('quarter', ih."date") ORDER BY ih."date" DESC) AS rn
    FROM engine.instance_historical ih
    WHERE ih.forecast_id = p_forecast_id
      AND ih.value IS NOT NULL
  ),
  lqm AS (
    SELECT
      forecast_id, qprev,
      max(v)  FILTER (WHERE rn = 1)   AS lqm1,
      avg(v)  FILTER (WHERE rn <= 5)  AS lqm5,
      avg(v)  FILTER (WHERE rn <= 10) AS lqm10,
      avg(v)  FILTER (WHERE rn <= 15) AS lqm15,
      avg(v)  FILTER (WHERE rn <= 30) AS lqm30
    FROM q_rank
    GROUP BY forecast_id, qprev
  ),
  m_rank AS (
    SELECT
      ih.forecast_id,
      date_trunc('month', ih."date")::date AS mprev,
      ih.value AS v,
      row_number() OVER (PARTITION BY ih.forecast_id, date_trunc('month', ih."date") ORDER BY ih."date" DESC) AS rn
    FROM engine.instance_historical ih
    WHERE ih.forecast_id = p_forecast_id
      AND ih.value IS NOT NULL
  ),
  lmm AS (
    SELECT
      forecast_id, mprev,
      max(v)  FILTER (WHERE rn = 1)   AS lmm1,
      avg(v)  FILTER (WHERE rn <= 5)  AS lmm5,
      avg(v)  FILTER (WHERE rn <= 10) AS lmm10,
      avg(v)  FILTER (WHERE rn <= 15) AS lmm15,
      avg(v)                          AS lmm30
    FROM m_rank
    GROUP BY forecast_id, mprev
  ),
  calc AS (
    SELECT
      r.d,
      /* qmv/mmv: same for every date in the quarter/month; NULL if value is NULL */
      CASE WHEN r.value IS NULL THEN NULL ELSE qa.qmv_all END AS qmv,
      CASE WHEN r.value IS NULL THEN NULL ELSE ma.mmv_all END AS mmv,
      /* LQM: NULL in first quarter; else from previous quarter summary */
      CASE WHEN r.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm1  END AS lqm1,
      CASE WHEN r.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm5  END AS lqm5,
      CASE WHEN r.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm10 END AS lqm10,
      CASE WHEN r.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm15 END AS lqm15,
      CASE WHEN r.qcur = b.first_quarter_start THEN NULL ELSE lq.lqm30 END AS lqm30,
      /* LMM: NULL in first month; NULL after allowed end; else from previous month summary */
      CASE WHEN r.mcur = b.first_month_start OR r.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm1  END AS lmm1,
      CASE WHEN r.mcur = b.first_month_start OR r.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm5  END AS lmm5,
      CASE WHEN r.mcur = b.first_month_start OR r.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm10 END AS lmm10,
      CASE WHEN r.mcur = b.first_month_start OR r.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm15 END AS lmm15,
      CASE WHEN r.mcur = b.first_month_start OR r.d > b.lmm_allowed_end THEN NULL ELSE lm.lmm30 END AS lmm30
    FROM rows r
    JOIN bounds b ON b.forecast_id = r.forecast_id
    LEFT JOIN qagg qa ON qa.forecast_id = r.forecast_id AND qa.qcur = r.qcur
    LEFT JOIN magg ma ON ma.forecast_id = r.forecast_id AND ma.mcur = r.mcur
    LEFT JOIN lqm  lq ON lq.forecast_id = r.forecast_id AND lq.qprev = (r.qcur - interval '3 months')::date
    LEFT JOIN lmm  lm ON lm.forecast_id = r.forecast_id AND lm.mprev = (r.mcur - interval '1 month')::date
  )
  UPDATE engine.instance_historical ih
  SET
    qmv  = c.qmv,
    mmv  = c.mmv,
    lqm1 = c.lqm1, lqm5 = c.lqm5, lqm10 = c.lqm10, lqm15 = c.lqm15, lqm30 = c.lqm30,
    lmm1 = c.lmm1, lmm5 = c.lmm5, lmm10 = c.lmm10, lmm15 = c.lmm15, lmm30 = c.lmm30
  FROM calc c
  WHERE ih.forecast_id = p_forecast_id
    AND ih."date" = c.d;

END;
$$;
