-- Replace with schema-accurate column names ("DATE","VALUE","ARIMA-Q", etc.)
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
    ORDER BY sh."DATE"
  ),
  w AS (
    SELECT
      s.*,
      avg(s.v) OVER (
        PARTITION BY s.forecast_id ORDER BY s.d
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ) AS qmv,
      avg(s.v) OVER (
        PARTITION BY s.forecast_id ORDER BY s.d
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS mmv
    FROM s
  ),
  l AS (
    SELECT
      w.*,
      lag(w.qmv,  1) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lqm1,
      lag(w.qmv,  5) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lqm5,
      lag(w.qmv, 10) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lqm10,
      lag(w.qmv, 15) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lqm15,
      lag(w.qmv, 30) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lqm30,
      lag(w.mmv,  1) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lmm1,
      lag(w.mmv,  5) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lmm5,
      lag(w.mmv, 10) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lmm10,
      lag(w.mmv, 15) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lmm15,
      lag(w.mmv, 30) OVER (PARTITION BY w.forecast_id ORDER BY w.d) AS lmm30
    FROM w
  )
  INSERT INTO engine.instance_historical (
    forecast_id, "date", value, qmv, mmv,
    lqm1, lqm5, lqm10, lqm15, lqm30,
    lmm1, lmm5, lmm10, lmm15, lmm30,
    arima_q, ses_q, hwes_q,
    arima_m, ses_m, hwes_m
  )
  SELECT
    l.forecast_id, l.d, l.v, l.qmv, l.mmv,
    l.lqm1, l.lqm5, l.lqm10, l.lqm15, l.lqm30,
    l.lmm1, l.lmm5, l.lmm10, l.lmm15, l.lmm30,
    l.arima_q, l.ses_q, l.hwes_q,
    l.arima_m, l.ses_m, l.hwes_m
  FROM l
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
