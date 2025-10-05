-- V14_10_Forecast_MSQM.sql
-- Generated: 2025-10-04
-- Purpose: MSQM forecast core per CANNON spec with destination schema creation (1:1 column set/types per MSQM pattern).
-- Notes:
--  - Identifier columns use TEXT (series, season, model, fmsr_series) as in MSQM.
--  - Measures use NUMERIC; id/date types preserved as UUID/DATE; created_at TIMESTAMPTZ.
--  - Primary key: (forecast_id, date, model_name, fmsr_series) — unchanged.
-- ===============================================================================================

CREATE OR REPLACE FUNCTION engine.msqm_forecast(forecast_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    latest_id uuid;
BEGIN
    SELECT fr.latest_instance_id INTO latest_id
    FROM engine.forecast_registry fr
    WHERE fr.forecast_name = msqm_forecast.forecast_name
    ORDER BY fr.created_at DESC
    LIMIT 1;

    IF latest_id IS NULL THEN
        RAISE EXCEPTION 'No forecast instance found for name=%', forecast_name;
    END IF;

    PERFORM engine.msqm_forecast__core(latest_id, forecast_name);
END;
$$;

CREATE OR REPLACE FUNCTION engine.msqm_forecast__core(latest_id uuid, forecast_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    dest_rel           text;
    series_col         text;
    season_col         text;
    sr_rel             text;
    dest_qual          text;
    sr_qual            text;
    msr_col_name       text;
BEGIN
    /* ---------- Resolve registry (no destination reads later) ---------- */
    SELECT fr.dest_table_name, fr.series_col, fr.season_col, fr.sr_table_name
      INTO dest_rel, series_col, season_col, sr_rel
    FROM engine.forecast_registry fr
    WHERE fr.latest_instance_id = latest_id
    ORDER BY fr.created_at DESC
    LIMIT 1;

    IF dest_rel IS NULL OR series_col IS NULL OR season_col IS NULL OR sr_rel IS NULL THEN
        RAISE EXCEPTION 'forecast_registry incomplete for % (dest/series/season/sr required)', forecast_name;
    END IF;

    dest_qual := format('%I.%I','engine', dest_rel);
    sr_qual   := format('%I.%I','engine', sr_rel);
    msr_col_name := format('%s_msr', replace(sr_rel, 'sr_', ''));

    /* ---------- Create destination table if missing (1:1 schema) ---------- */
    EXECUTE format($DDL$
      CREATE TABLE IF NOT EXISTS %s (
        forecast_id uuid NOT NULL,
        date date NOT NULL,
        value numeric,
        %1$I text,                -- series col
        %2$I text,                -- season col (_yqm)
        model_name text,
        base_model text,
        base_fv numeric,
        fmsr_series text,
        fmsr_value numeric,
        fv numeric,
        fv_error numeric,
        -- MAPE family
        fv_mape numeric,
        fv_mean_mape numeric,
        fv_mean_mape_c numeric,
        -- 10-band bounds
        fv_b125_u numeric, fv_b125_l numeric,
        fv_b150_u numeric, fv_b150_l numeric,
        fv_b175_u numeric, fv_b175_l numeric,
        fv_b200_u numeric, fv_b200_l numeric,
        fv_b225_u numeric, fv_b225_l numeric,
        fv_b250_u numeric, fv_b250_l numeric,
        fv_b275_u numeric, fv_b275_l numeric,
        fv_b300_u numeric, fv_b300_l numeric,
        fv_b325_u numeric, fv_b325_l numeric,
        fv_b350_u numeric, fv_b350_l numeric,
        -- Per-row hits
        b125_hit text, b150_hit text, b175_hit text, b200_hit text, b225_hit text,
        b250_hit text, b275_hit text, b300_hit text, b325_hit text, b350_hit text,
        -- Historical coverage (mean of prior season-band scores)
        b125_cov numeric, b150_cov numeric, b175_cov numeric, b200_cov numeric, b225_cov numeric,
        b250_cov numeric, b275_cov numeric, b300_cov numeric, b325_cov numeric, b350_cov numeric,
        -- Selected CIs
        ci85_low numeric, ci85_high numeric,
        ci90_low numeric, ci90_high numeric,
        ci95_low numeric, ci95_high numeric,
        -- MAE & RMSE families
        fv_mae numeric,
        fv_mean_mae numeric, fv_mean_mae_c numeric,
        fv_rmse numeric,
        fv_mean_rmse numeric, fv_mean_rmse_c numeric,
        -- A0/Ax comparisons (MAPE/MAE/RMSE)
        mape_comparison text, mean_mape_comparison text, accuracy_comparison text,
        mae_comparison  text, mean_mae_comparison  text, mae_accuracy_comparison text,
        rmse_comparison text, mean_rmse_comparison text, rmse_accuracy_comparison text,
        -- Per-metric prior-season win counts
        best_mape_count numeric, best_mae_count numeric, best_rmse_count numeric,
        -- Variability
        %3$I numeric,            -- <table>_msr
        msr_dir text, fmsr_dir text, dir_hit text,
        dir_hit_count numeric,
        -- Variance summary (kept for parity)
        fv_variance numeric, fv_variance_mean numeric,
        created_at timestamptz DEFAULT now(),
        CONSTRAINT %4$I PRIMARY KEY (forecast_id, date, model_name, fmsr_series)
      )
    $DDL$, dest_qual, series_col, msr_col_name, dest_rel||'_pk');

    /* ---------- Perf settings ---------- */
    PERFORM set_config('work_mem','256MB',true);
    PERFORM set_config('maintenance_work_mem','512MB',true);

    /* ---------- Universe ---------- */
    EXECUTE $ctas$
      CREATE TEMP TABLE __universe ON COMMIT DROP AS
      SELECT *
      FROM engine.instance_historical
      WHERE forecast_id = $1
    $ctas$ USING latest_id;
    ANALYZE __universe;

    /* ---------- Work table (LIKE dest) ---------- */
    EXECUTE format('CREATE TEMP TABLE __work (LIKE %s INCLUDING ALL);', dest_qual);
    EXECUTE format('CREATE INDEX ON __work (%I, %I);', series_col, season_col);
    ANALYZE __work;

    /* ---------- Pass 1: Hydration ---------- */
    EXECUTE format($i$
      INSERT INTO __work
      (
        forecast_id, date, value, %1$I, %2$I, model_name, base_model, base_fv, fmsr_series, fmsr_value,
        fv, fv_error, fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l,
        fv_b225_u, fv_b225_l, fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l,
        fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison,
        best_mape_count, best_mae_count, best_rmse_count,
        %3$I, msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        fv_variance, fv_variance_mean, created_at
      )
      SELECT
         u.forecast_id, u.date, u.value, u.%1$I, u.%2$I, u.model_name, u.base_model, u.base_fv, u.fmsr_series, u.fmsr_value,
         u.fv, ABS(u.value - u.fv) AS fv_error, NULL, NULL, NULL,
         NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL,
         NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL,
         NULL,NULL, NULL,NULL,
         NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
         NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
         NULL,NULL, NULL,NULL, NULL,NULL,
         NULL,NULL,NULL,
         NULL,NULL,NULL,
         NULL,NULL,NULL,
         s.%3$I, NULL, NULL, NULL, NULL,
         NULL, NULL, now()
      FROM __universe u
      LEFT JOIN %4$s s
        ON s.%2$I = u.%2$I
      $i$, series_col, season_col, msr_col_name, sr_qual);

    ANALYZE __work;

    /* ---------- Pass 2: Row metrics ---------- */
    UPDATE __work
    SET fv_mape = CASE WHEN abs(value) > 1e-12 THEN abs(value - fv) / abs(value) ELSE NULL END,
        fv_mae  = abs(value - fv);

    /* ---------- Pass 3: Season anatomy ---------- */
    EXECUTE format($q$
      CREATE TEMP TABLE __season_dim AS
      SELECT
        model_name,
        %1$I AS series,
        %2$I AS season,
        MIN(date) AS season_start,
        AVG(fv_mape) AS season_mape,
        AVG(fv_mae)  AS season_mae,
        SQRT(AVG(POWER(value - fv,2))) AS season_rmse,
        MAX(%3$I) AS season_msr,
        MAX(fmsr_value) AS season_fmsr
      FROM __work
      GROUP BY model_name, %1$I, %2$I
    $q$, series_col, season_col, msr_col_name);
    CREATE INDEX ON __season_dim (model_name, season_start);
    ANALYZE __season_dim;

    /* ---------- Pass 4: Rolling means + variability ---------- */
    CREATE TEMP TABLE __season_hist AS
    SELECT
      sd.*,
      AVG(sd.season_mape)  OVER w_excl AS mean_mape_hist,
      COUNT(sd.season_mape) OVER w_excl AS mean_mape_hist_c,
      AVG(sd.season_mae)   OVER w_excl AS mean_mae_hist,
      COUNT(sd.season_mae) OVER w_excl AS mean_mae_hist_c,
      AVG(sd.season_rmse)  OVER w_excl AS mean_rmse_hist,
      COUNT(sd.season_rmse)OVER w_excl AS mean_rmse_hist_c,
      LAG(sd.season_msr)   OVER w_full AS prev_msr,
      LAG(sd.season_fmsr)  OVER w_full AS prev_fmsr
    FROM __season_dim sd
    WINDOW
      w_excl AS (PARTITION BY sd.model_name ORDER BY sd.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      w_full AS (PARTITION BY sd.model_name ORDER BY sd.season_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
    CREATE INDEX ON __season_hist (model_name, season_start);
    ANALYZE __season_hist;

    EXECUTE format($u$
      UPDATE __work w
      SET fv_mean_mape  = sh.mean_mape_hist,
          fv_mean_mape_c= sh.mean_mape_hist_c,
          fv_mean_mae   = sh.mean_mae_hist,
          fv_mean_mae_c = sh.mean_mae_hist_c,
          fv_mean_rmse  = sh.mean_rmse_hist,
          fv_mean_rmse_c= sh.mean_rmse_hist_c
      FROM __season_hist sh
      WHERE w.model_name = sh.model_name AND w.%1$I = sh.series AND w.%2$I = sh.season
    $u$, series_col, season_col);

    WITH dirs AS (
      SELECT
        model_name, series, season, season_start,
        CASE WHEN prev_msr  IS NULL OR season_msr  = prev_msr  THEN NULL
             WHEN season_msr  > prev_msr  THEN 'U' ELSE 'D' END AS msr_dir,
        CASE WHEN prev_fmsr IS NULL OR season_fmsr = prev_fmsr THEN NULL
             WHEN season_fmsr > prev_fmsr THEN 'U' ELSE 'D' END AS fmsr_dir
      FROM __season_hist
    ),
    dir_hits AS (
      SELECT d.*,
             CASE WHEN d.msr_dir IS NOT NULL AND d.fmsr_dir IS NOT NULL AND d.msr_dir = d.fmsr_dir THEN 'Y'
                  WHEN d.msr_dir IS NOT NULL AND d.fmsr_dir IS NOT NULL AND d.msr_dir <> d.fmsr_dir THEN 'N'
                  ELSE NULL END AS dir_hit
      FROM dirs d
    ),
    dir_counts AS (
      SELECT
        model_name, series, season, season_start, msr_dir, fmsr_dir, dir_hit,
        SUM( CASE WHEN dir_hit='Y' THEN 1 ELSE 0 END )
          OVER (PARTITION BY model_name ORDER BY season_start
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS dir_hit_count
      FROM dir_hits
    )
    EXECUTE format($ud$
      UPDATE __work w
      SET msr_dir = dc.msr_dir, fmsr_dir = dc.fmsr_dir, dir_hit = dc.dir_hit, dir_hit_count = dc.dir_hit_count
      FROM dir_counts dc
      WHERE w.model_name = dc.model_name AND w.%1$I = dc.series AND w.%2$I = dc.season
    $ud$, series_col, season_col);

    /* ---------- Pass 5: Bands/coverage ---------- */
    UPDATE __work
    SET
      fv_b125_u = fv * (1 + COALESCE(fv_mean_mape,0) * 1.25), fv_b125_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 1.25)),
      fv_b150_u = fv * (1 + COALESCE(fv_mean_mape,0) * 1.50), fv_b150_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 1.50)),
      fv_b175_u = fv * (1 + COALESCE(fv_mean_mape,0) * 1.75), fv_b175_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 1.75)),
      fv_b200_u = fv * (1 + COALESCE(fv_mean_mape,0) * 2.00), fv_b200_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 2.00)),
      fv_b225_u = fv * (1 + COALESCE(fv_mean_mape,0) * 2.25), fv_b225_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 2.25)),
      fv_b250_u = fv * (1 + COALESCE(fv_mean_mape,0) * 2.50), fv_b250_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 2.50)),
      fv_b275_u = fv * (1 + COALESCE(fv_mean_mape,0) * 2.75), fv_b275_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 2.75)),
      fv_b300_u = fv * (1 + COALESCE(fv_mean_mape,0) * 3.00), fv_b300_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 3.00)),
      fv_b325_u = fv * (1 + COALESCE(fv_mean_mape,0) * 3.25), fv_b325_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 3.25)),
      fv_b350_u = fv * (1 + COALESCE(fv_mean_mape,0) * 3.50), fv_b350_l = GREATEST(0, fv * (1 - COALESCE(fv_mean_mape,0) * 3.50));

    UPDATE __work
    SET b125_hit = CASE WHEN value > fv_b125_l AND value < fv_b125_u THEN 'Y' ELSE 'N' END,
        b150_hit = CASE WHEN value > fv_b150_l AND value < fv_b150_u THEN 'Y' ELSE 'N' END,
        b175_hit = CASE WHEN value > fv_b175_l AND value < fv_b175_u THEN 'Y' ELSE 'N' END,
        b200_hit = CASE WHEN value > fv_b200_l AND value < fv_b200_u THEN 'Y' ELSE 'N' END,
        b225_hit = CASE WHEN value > fv_b225_l AND value < fv_b225_u THEN 'Y' ELSE 'N' END,
        b250_hit = CASE WHEN value > fv_b250_l AND value < fv_b250_u THEN 'Y' ELSE 'N' END,
        b275_hit = CASE WHEN value > fv_b275_l AND value < fv_b275_u THEN 'Y' ELSE 'N' END,
        b300_hit = CASE WHEN value > fv_b300_l AND value < fv_b300_u THEN 'Y' ELSE 'N' END,
        b325_hit = CASE WHEN value > fv_b325_l AND value < fv_b325_u THEN 'Y' ELSE 'N' END,
        b350_hit = CASE WHEN value > fv_b350_l AND value < fv_b350_u THEN 'Y' ELSE 'N' END;

    EXECUTE format($sb$
      CREATE TEMP TABLE __season_bands AS
      SELECT
        model_name, %1$I AS series, %2$I AS season,
        MIN(date) AS season_start,
        AVG( (b125_hit='Y')::int )::numeric AS s125,
        AVG( (b150_hit='Y')::int )::numeric AS s150,
        AVG( (b175_hit='Y')::int )::numeric AS s175,
        AVG( (b200_hit='Y')::int )::numeric AS s200,
        AVG( (b225_hit='Y')::int )::numeric AS s225,
        AVG( (b250_hit='Y')::int )::numeric AS s250,
        AVG( (b275_hit='Y')::int )::numeric AS s275,
        AVG( (b300_hit='Y')::int )::numeric AS s300,
        AVG( (b325_hit='Y')::int )::numeric AS s325,
        AVG( (b350_hit='Y')::int )::numeric AS s350
      FROM __work
      GROUP BY model_name, %1$I, %2$I
    $sb$, series_col, season_col);
    CREATE INDEX ON __season_bands (model_name, season_start);
    ANALYZE __season_bands;

    CREATE TEMP TABLE __band_cov AS
    SELECT
      model_name, series, season, season_start,
      AVG(s125) OVER w AS c125,
      AVG(s150) OVER w AS c150,
      AVG(s175) OVER w AS c175,
      AVG(s200) OVER w AS c200,
      AVG(s225) OVER w AS c225,
      AVG(s250) OVER w AS c250,
      AVG(s275) OVER w AS c275,
      AVG(s300) OVER w AS c300,
      AVG(s325) OVER w AS c325,
      AVG(s350) OVER w AS c350
    FROM __season_bands
    WINDOW w AS (PARTITION BY model_name ORDER BY season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING);
    CREATE INDEX ON __band_cov (model_name, season_start);
    ANALYZE __band_cov;

    EXECUTE format($wc$
      UPDATE __work w
      SET b125_cov = bc.c125, b150_cov = bc.c150, b175_cov = bc.c175, b200_cov = bc.c200, b225_cov = bc.c225,
          b250_cov = bc.c250, b275_cov = bc.c275, b300_cov = bc.c300, b325_cov = bc.c325, b350_cov = bc.c350
      FROM __band_cov bc
      WHERE w.model_name = bc.model_name AND w.%1$I = bc.series AND w.%2$I = bc.season
    $wc$, series_col, season_col);

    /* ---------- Pass 6: CI selection (abundance + dearth rules) ---------- */
    CREATE TEMP TABLE __ci_choice AS
    WITH ranked AS (
      SELECT
        bc.model_name, sb.series, sb.season, bc.season_start,
        ARRAY[1.25,1.50,1.75,2.00,2.25,2.50,2.75,3.00,3.25,3.50]::numeric[] AS bands,
        ARRAY[bc.c125,bc.c150,bc.c175,bc.c200,bc.c225,bc.c250,bc.c275,bc.c300,bc.c325,bc.c350]::numeric[] AS covs
      FROM __band_cov bc
      JOIN __season_bands sb USING (model_name, series, season, season_start)
    ), picked AS (
      SELECT
        model_name, series, season, season_start,
        COALESCE( (SELECT bands[i] FROM generate_series(1,10) i WHERE covs[i] >= 0.85 ORDER BY i LIMIT 1), 3.00 ) AS b85,
        NULL::numeric AS b90,
        NULL::numeric AS b95,
        bands, covs
      FROM ranked
    ), picked2 AS (
      SELECT
        model_name, series, season, season_start,
        b85,
        COALESCE( (SELECT bands[j] FROM generate_series(1,10) j WHERE bands[j] > b85 AND covs[j] >= 0.90 ORDER BY j LIMIT 1),
                  LEAST( (SELECT bands[j] FROM generate_series(1,10) j WHERE bands[j] > b85 ORDER BY j LIMIT 1), 3.25) ) AS b90,
        bands, covs
      FROM picked
    ), picked3 AS (
      SELECT
        model_name, series, season, season_start,
        b85, b90,
        COALESCE( (SELECT bands[k] FROM generate_series(1,10) k WHERE bands[k] > b90 AND covs[k] >= 0.95 ORDER BY k LIMIT 1),
                  LEAST( (SELECT bands[k] FROM generate_series(1,10) k WHERE bands[k] > b90 ORDER BY k LIMIT 1), 3.50) ) AS b95
      FROM picked2
    )
    SELECT * FROM picked3;
    CREATE INDEX ON __ci_choice (model_name, season_start);
    ANALYZE __ci_choice;

    EXECUTE format($ci$
      UPDATE __work w
      SET ci85_low = CASE cic.b85
                       WHEN 1.25 THEN w.fv_b125_l WHEN 1.50 THEN w.fv_b150_l WHEN 1.75 THEN w.fv_b175_l
                       WHEN 2.00 THEN w.fv_b200_l WHEN 2.25 THEN w.fv_b225_l WHEN 2.50 THEN w.fv_b250_l
                       WHEN 2.75 THEN w.fv_b275_l WHEN 3.00 THEN w.fv_b300_l WHEN 3.25 THEN w.fv_b325_l
                       WHEN 3.50 THEN w.fv_b350_l END,
          ci85_high = CASE cic.b85
                       WHEN 1.25 THEN w.fv_b125_u WHEN 1.50 THEN w.fv_b150_u WHEN 1.75 THEN w.fv_b175_u
                       WHEN 2.00 THEN w.fv_b200_u WHEN 2.25 THEN w.fv_b225_u WHEN 2.50 THEN w.fv_b250_u
                       WHEN 2.75 THEN w.fv_b275_u WHEN 3.00 THEN w.fv_b300_u WHEN 3.25 THEN w.fv_b325_u
                       WHEN 3.50 THEN w.fv_b350_u END,
          ci90_low = CASE cic.b90
                       WHEN 1.25 THEN w.fv_b125_l WHEN 1.50 THEN w.fv_b150_l WHEN 1.75 THEN w.fv_b175_l
                       WHEN 2.00 THEN w.fv_b200_l WHEN 2.25 THEN w.fv_b225_l WHEN 2.50 THEN w.fv_b250_l
                       WHEN 2.75 THEN w.fv_b275_l WHEN 3.00 THEN w.fv_b300_l WHEN 3.25 THEN w.fv_b325_l
                       WHEN 3.50 THEN w.fv_b350_l END,
          ci90_high = CASE cic.b90
                       WHEN 1.25 THEN w.fv_b125_u WHEN 1.50 THEN w.fv_b150_u WHEN 1.75 THEN w.fv_b175_u
                       WHEN 2.00 THEN w.fv_b200_u WHEN 2.25 THEN w.fv_b225_u WHEN 2.50 THEN w.fv_b250_u
                       WHEN 2.75 THEN w.fv_b275_u WHEN 3.00 THEN w.fv_b300_u WHEN 3.25 THEN w.fv_b325_u
                       WHEN 3.50 THEN w.fv_b350_u END,
          ci95_low = CASE cic.b95
                       WHEN 1.25 THEN w.fv_b125_l WHEN 1.50 THEN w.fv_b150_l WHEN 1.75 THEN w.fv_b175_l
                       WHEN 2.00 THEN w.fv_b200_l WHEN 2.25 THEN w.fv_b225_l WHEN 2.50 THEN w.fv_b250_l
                       WHEN 2.75 THEN w.fv_b275_l WHEN 3.00 THEN w.fv_b300_l WHEN 3.25 THEN w.fv_b325_l
                       WHEN 3.50 THEN w.fv_b350_l END,
          ci95_high = CASE cic.b95
                       WHEN 1.25 THEN w.fv_b125_u WHEN 1.50 THEN w.fv_b150_u WHEN 1.75 THEN w.fv_b175_u
                       WHEN 2.00 THEN w.fv_b200_u WHEN 2.25 THEN w.fv_b225_u WHEN 2.50 THEN w.fv_b250_u
                       WHEN 2.75 THEN w.fv_b275_u WHEN 3.00 THEN w.fv_b300_u WHEN 3.25 THEN w.fv_b325_u
                       WHEN 3.50 THEN w.fv_b350_u END
      FROM __ci_choice cic
      WHERE w.model_name = cic.model_name AND w.%1$I = cic.series AND w.%2$I = cic.season
    $ci$, series_col, season_col);

    /* ---------- Pass 7: A0/Ax + per-metric counts ---------- */
    EXECUTE format($a0$
      CREATE TEMP TABLE __a0_cache AS
      SELECT
        sd.%1$I AS series, sd.%2$I AS season, sd.season_start,
        sd.model_name AS a0_model,
        sd.season_mape AS a0_season_mape,
        sh.mean_mape_hist AS a0_mean_mape,
        sd.season_mae  AS a0_season_mae,
        sh.mean_mae_hist AS a0_mean_mae,
        sd.season_rmse AS a0_season_rmse,
        sh.mean_rmse_hist AS a0_mean_rmse
      FROM __season_dim sd
      JOIN __season_hist sh USING (model_name, series, season, season_start)
      WHERE sd.model_name = 'A0'
    $a0$, series_col, season_col);
    CREATE INDEX ON __a0_cache (series, season);
    ANALYZE __a0_cache;

    WITH cmp AS (
      SELECT
        ax.model_name, ax.series, ax.season, ax.season_start,
        ax.season_mape, ax.season_mae, ax.season_rmse,
        h.mean_mape_hist AS ax_mean_mape,
        h.mean_mae_hist  AS ax_mean_mae,
        h.mean_rmse_hist AS ax_mean_rmse,
        a0.a0_season_mape, a0.a0_mean_mape,
        a0.a0_season_mae,  a0.a0_mean_mae,
        a0.a0_season_rmse, a0.a0_mean_rmse
      FROM __season_dim ax
      JOIN __season_hist h USING (model_name, series, season, season_start)
      JOIN __a0_cache a0 USING (series, season)
      WHERE ax.model_name <> 'A0'
    )
    EXECUTE format($uc$
      UPDATE __work w
      SET mape_comparison = CASE WHEN c.season_mape    < c.a0_season_mape THEN 'L' ELSE 'H' END,
          mean_mape_comparison = CASE WHEN c.ax_mean_mape  < c.a0_mean_mape   THEN 'L' ELSE 'H' END,
          accuracy_comparison  = CASE WHEN c.season_mape    < c.a0_season_mape
                                    AND c.ax_mean_mape  < c.a0_mean_mape   THEN 'Y' ELSE 'N' END,
          mae_comparison       = CASE WHEN c.season_mae     < c.a0_season_mae  THEN 'L' ELSE 'H' END,
          mean_mae_comparison  = CASE WHEN c.ax_mean_mae    < c.a0_mean_mae    THEN 'L' ELSE 'H' END,
          mae_accuracy_comparison = CASE WHEN c.season_mae  < c.a0_season_mae
                                    AND c.ax_mean_mae  < c.a0_mean_mae    THEN 'Y' ELSE 'N' END,
          rmse_comparison      = CASE WHEN c.season_rmse    < c.a0_season_rmse THEN 'L' ELSE 'H' END,
          mean_rmse_comparison = CASE WHEN c.ax_mean_rmse   < c.a0_mean_rmse   THEN 'L' ELSE 'H' END,
          rmse_accuracy_comparison = CASE WHEN c.season_rmse < c.a0_season_rmse
                                    AND c.ax_mean_rmse < c.a0_mean_rmse  THEN 'Y' ELSE 'N' END
      FROM cmp c
      WHERE w.model_name = c.model_name AND w.%1$I = c.series AND w.%2$I = c.season
    $uc$, series_col, season_col);

    WITH season_flags AS (
      SELECT
        sh.model_name, sh.series, sh.season, sh.season_start,
        MAX( (w.accuracy_comparison='Y')::int ) AS acc_mape_y,
        MAX( (w.mae_accuracy_comparison='Y')::int ) AS acc_mae_y,
        MAX( (w.rmse_accuracy_comparison='Y')::int ) AS acc_rmse_y
      FROM __season_hist sh
      JOIN __work w ON (w.model_name=sh.model_name AND w.%1$I=sh.series AND w.%2$I=sh.season)
      GROUP BY sh.model_name, sh.series, sh.season, sh.season_start
    ), running AS (
      SELECT
        model_name, series, season, season_start,
        SUM(acc_mape_y) OVER (PARTITION BY model_name ORDER BY season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mape_count,
        SUM(acc_mae_y)  OVER (PARTITION BY model_name ORDER BY season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_mae_count,
        SUM(acc_rmse_y) OVER (PARTITION BY model_name ORDER BY season_start ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_rmse_count
      FROM season_flags
    )
    EXECUTE format($wr$
      UPDATE __work w
      SET best_mape_count = r.best_mape_count,
          best_mae_count  = r.best_mae_count,
          best_rmse_count = r.best_rmse_count
      FROM running r
      WHERE w.model_name = r.model_name AND w.%1$I = r.series AND w.%2$I = r.season
    $wr$, series_col, season_col);

    /* ---------- Pass 8: Final rounding & explicit INSERT column list ---------- */
    UPDATE __work
    SET fv_mape = ROUND(fv_mape::numeric, 4),
        fv_mae  = ROUND(fv_mae::numeric,  4),
        fv_rmse = ROUND(fv_rmse::numeric, 4),
        fv_mean_mape = ROUND(fv_mean_mape::numeric, 4),
        fv_mean_mae  = ROUND(fv_mean_mae::numeric, 4),
        fv_mean_rmse = ROUND(fv_mean_rmse::numeric, 4),
        b125_cov = ROUND(b125_cov::numeric, 4), b150_cov = ROUND(b150_cov::numeric, 4),
        b175_cov = ROUND(b175_cov::numeric, 4), b200_cov = ROUND(b200_cov::numeric, 4),
        b225_cov = ROUND(b225_cov::numeric, 4), b250_cov = ROUND(b250_cov::numeric, 4),
        b275_cov = ROUND(b275_cov::numeric, 4), b300_cov = ROUND(b300_cov::numeric, 4),
        b325_cov = ROUND(b325_cov::numeric, 4), b350_cov = ROUND(b350_cov::numeric, 4),
        ci85_low = ROUND(ci85_low::numeric, 4), ci85_high = ROUND(ci85_high::numeric, 4),
        ci90_low = ROUND(ci90_low::numeric, 4), ci90_high = ROUND(ci90_high::numeric, 4),
        ci95_low = ROUND(ci95_low::numeric, 4), ci95_high = ROUND(ci95_high::numeric, 4);

    EXECUTE format($ins$
      INSERT INTO %s (
        forecast_id, date, value, %1$I, %2$I, model_name, base_model, base_fv, fmsr_series, fmsr_value,
        fv, fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l,
        fv_b225_u, fv_b225_l, fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l,
        fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison,
        best_mape_count, best_mae_count, best_rmse_count,
        %3$I, msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        fv_variance, fv_variance_mean, created_at
      )
      SELECT
        forecast_id, date, value, %1$I, %2$I, model_name, base_model, base_fv, fmsr_series, fmsr_value,
        fv, fv_error,
        fv_mape, fv_mean_mape, fv_mean_mape_c,
        fv_b125_u, fv_b125_l, fv_b150_u, fv_b150_l, fv_b175_u, fv_b175_l, fv_b200_u, fv_b200_l,
        fv_b225_u, fv_b225_l, fv_b250_u, fv_b250_l, fv_b275_u, fv_b275_l, fv_b300_u, fv_b300_l,
        fv_b325_u, fv_b325_l, fv_b350_u, fv_b350_l,
        b125_hit, b150_hit, b175_hit, b200_hit, b225_hit, b250_hit, b275_hit, b300_hit, b325_hit, b350_hit,
        b125_cov, b150_cov, b175_cov, b200_cov, b225_cov, b250_cov, b275_cov, b300_cov, b325_cov, b350_cov,
        ci85_low, ci85_high, ci90_low, ci90_high, ci95_low, ci95_high,
        fv_mae, fv_mean_mae, fv_mean_mae_c,
        fv_rmse, fv_mean_rmse, fv_mean_rmse_c,
        mape_comparison, mean_mape_comparison, accuracy_comparison,
        mae_comparison, mean_mae_comparison, mae_accuracy_comparison,
        rmse_comparison, mean_rmse_comparison, rmse_accuracy_comparison,
        best_mape_count, best_mae_count, best_rmse_count,
        %3$I, msr_dir, fmsr_dir, dir_hit, dir_hit_count,
        fv_variance, fv_variance_mean, created_at
      FROM __work
      WHERE forecast_id = $1
    $ins$, dest_qual, series_col, season_col, msr_col_name) USING latest_id;

END;
$$;
