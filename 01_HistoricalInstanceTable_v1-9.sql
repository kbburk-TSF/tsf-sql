-- =====================================================================
-- Version: 2025-09-11  v1.9 (engine.build_instance_historical)
-- EXACT transplant of validated SELECT; ONLY changes:
--   • Source table = engine.staging_historical (filtered by p_forecast_id)
--   • Target table = engine.instance_historical (UPSERT)
--   • Adds created_at
-- =====================================================================

DROP FUNCTION IF EXISTS engine.build_instance_historical(uuid);

CREATE OR REPLACE FUNCTION engine.build_instance_historical(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  WITH s AS (
    SELECT
      sh.forecast_id,
      sh."DATE"::date                AS d,
      sh."VALUE"::double precision   AS v,
      sh."ARIMA-Q"::double precision AS arima_q,
      sh."SES-Q"::double precision   AS ses_q,
      sh."HWES-Q"::double precision  AS hwes_q,
      sh."ARIMA-M"::double precision AS arima_m,
      sh."SES-M"::double precision   AS ses_m,
      sh."HWES-M"::double precision  AS hwes_m
    FROM engine.staging_historical sh
    WHERE sh.forecast_id = p_forecast_id
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
  ),
  final AS (
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
  )
  INSERT INTO engine.instance_historical (
    forecast_id, "date", value, qmv, mmv,
    lqm1, lqm5, lqm10, lqm15, lqm30,
    lmm1, lmm5, lmm10, lmm15, lmm30,
    arima_q, ses_q, hwes_q,
    arima_m, ses_m, hwes_m,
    created_at
  )
  SELECT
    f.forecast_id, f."date", f.value, f.qmv, f.mmv,
    f.lqm1, f.lqm5, f.lqm10, f.lqm15, f.lqm30,
    f.lmm1, f.lmm5, f.lmm10, f.lmm15, f.lmm30,
    f.arima_q, f.ses_q, f.hwes_q,
    f.arima_m, f.ses_m, f.hwes_m,
    f.created_at
  FROM final f
  ON CONFLICT (forecast_id, "date") DO UPDATE SET
    value   = EXCLUDED.value,
    qmv     = EXCLUDED.qmv,
    mmv     = EXCLUDED.mmv,
    lqm1    = EXCLUDED.lqm1,  lqm5  = EXCLUDED.lqm5,
    lqm10   = EXCLUDED.lqm10, lqm15 = EXCLUDED.lqm15, lqm30 = EXCLUDED.lqm30,
    lmm1    = EXCLUDED.lmm1,  lmm5  = EXCLUDED.lmm5,
    lmm10   = EXCLUDED.lmm10, lmm15 = EXCLUDED.lmm15, lmm30 = EXCLUDED.lmm30,
    arima_q = EXCLUDED.arima_q, ses_q = EXCLUDED.ses_q, hwes_q = EXCLUDED.hwes_q,
    arima_m = EXCLUDED.arima_m, ses_m = EXCLUDED.ses_m, hwes_m = EXCLUDED.hwes_m;
END;
$$;
