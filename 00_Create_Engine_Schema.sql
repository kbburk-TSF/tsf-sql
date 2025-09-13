-- 00_CREATE_ENGINE_SCHEMA.sql
-- Canon engine schema: ownership, global grants, default privileges, core tables,
-- and commit-only triggers that use temp tables (SECURITY INVOKER).
-- VC 1.0 (2025-09-12): Initial nuclear reset — make `engine` authoritative and fix perms end-to-end.

DO $$
DECLARE
  -- Set the canonical owner for EVERYTHING in the engine schema.
  -- This role will own the schema, core tables, and (later) the heavy builder functions.
  owner_role   text := current_user;

  -- Roles that should be able to read/write engine tables and call functions.
  writer_roles text[] := ARRAY['public'];

  r text;
BEGIN
  -- [0] SCHEMA OWNERSHIP (authoritative)
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'engine') THEN
    EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', 'engine', owner_role);
  ELSE
    EXECUTE format('ALTER SCHEMA %I OWNER TO %I', 'engine', owner_role);
  END IF;

  -- Writers can use the schema (but not create objects).
  FOREACH r IN ARRAY writer_roles LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA engine TO %I', r);
  END LOOP;

  -- [1] CORE TABLES (idempotent creates under the canonical owner)
  -- Drop+create is NOT used here; these are canonical. CREATE IF NOT EXISTS avoids accidental loss.
  EXECUTE $DDL$
    CREATE TABLE IF NOT EXISTS engine.staging_historical (
      forecast_id   uuid NOT NULL,
      forecast_name text,
      "DATE"        date,
      "VALUE"       double precision,
      "SES-M"       double precision, "SES-Q" double precision,
      "HWES-M"      double precision, "HWES-Q" double precision,
      "ARIMA-M"     double precision, "ARIMA-Q" double precision,
      uploaded_at   timestamptz DEFAULT now()
    );
  DDL$;
  EXECUTE format('ALTER TABLE engine.staging_historical OWNER TO %I', owner_role);

  EXECUTE $DDL$
    CREATE TABLE IF NOT EXISTS engine.forecast_registry (
      forecast_id         uuid PRIMARY KEY,
      forecast_name       text NOT NULL,
      source_csv_filename text NOT NULL,
      parameters          jsonb DEFAULT '{}'::jsonb,
      status              text  DEFAULT 'pending',
      created_at          timestamptz DEFAULT now()
    );
  DDL$;
  EXECUTE format('ALTER TABLE engine.forecast_registry OWNER TO %I', owner_role);

  EXECUTE $DDL$
    CREATE TABLE IF NOT EXISTS engine.instance_historical (
      forecast_id uuid NOT NULL,
      "date"      date NOT NULL,
      value       double precision,
      qmv         double precision,
      mmv         double precision,
      lqm1  double precision, lqm5  double precision, lqm10 double precision,
      lqm15 double precision, lqm30 double precision,
      lmm1  double precision, lmm5  double precision, lmm10 double precision,
      lmm15 double precision, lmm30 double precision,
      arima_q double precision, ses_q  double precision, hwes_q double precision,
      arima_m double precision, ses_m  double precision, hwes_m double precision,
      created_at timestamptz DEFAULT now(),
      PRIMARY KEY (forecast_id, "date")
    );
  DDL$;
  EXECUTE format('ALTER TABLE engine.instance_historical OWNER TO %I', owner_role);

  -- [2] INDEXES (idempotent)
  EXECUTE 'CREATE INDEX IF NOT EXISTS ix_staging_hist_fid_date ON engine.staging_historical (forecast_id, "DATE")';
  EXECUTE 'CREATE INDEX IF NOT EXISTS ix_hist_fid_date          ON engine.instance_historical (forecast_id, "date")';
  EXECUTE 'CREATE INDEX IF NOT EXISTS ix_hist_created_at        ON engine.instance_historical (created_at)';

  -- [3] GLOBAL GRANTS ON EXISTING OBJECTS
  FOREACH r IN ARRAY writer_roles LOOP
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA engine TO %I', r);
    EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA engine TO %I', r);
    EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO %I', r);
  END LOOP;

  -- [4] DEFAULT PRIVILEGES FOR FUTURE OBJECTS **CREATED BY owner_role** IN THIS SCHEMA
  -- These ensure later tables/indexes/functions created by our builder functions are usable by writers.
  FOREACH r IN ARRAY writer_roles LOOP
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA engine GRANT SELECT, INSERT, UPDATE, DELETE, TRIGGER, REFERENCES ON TABLES TO %I',
      owner_role, r
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA engine GRANT USAGE, SELECT ON SEQUENCES TO %I',
      owner_role, r
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA engine GRANT EXECUTE ON FUNCTIONS TO %I',
      owner_role, r
    );
  END LOOP;

  -- [5] CLEAN OLD SENTINELS/TRIGGERS (idempotent)
  EXECUTE 'DROP TRIGGER  IF EXISTS trg_collect_ids_stmt   ON engine.staging_historical';
  EXECUTE 'DROP TRIGGER  IF EXISTS trg_on_staging_commit  ON engine._tx_sentinel_staging';
  EXECUTE 'DROP FUNCTION IF EXISTS engine.trg_collect_staging_stmt()    CASCADE';
  EXECUTE 'DROP FUNCTION IF EXISTS engine.trg_finalize_staging_commit() CASCADE';

  EXECUTE 'DROP TRIGGER  IF EXISTS trg_collect_hist_stmt   ON engine.instance_historical';
  EXECUTE 'DROP TRIGGER  IF EXISTS trg_on_hist_commit      ON engine._tx_sentinel_hist';
  EXECUTE 'DROP FUNCTION IF EXISTS engine.trg_collect_hist_stmt()       CASCADE';
  EXECUTE 'DROP FUNCTION IF EXISTS engine.trg_finalize_hist_commit()    CASCADE';

  EXECUTE 'DROP TABLE IF EXISTS engine._tx_sentinel_staging';
  EXECUTE 'DROP TABLE IF EXISTS engine._tx_sentinel_hist';

  -- [6] SENTINEL TABLES (owned by owner_role)
  EXECUTE 'CREATE TABLE engine._tx_sentinel_staging (txid bigint PRIMARY KEY)';
  EXECUTE 'CREATE TABLE engine._tx_sentinel_hist    (txid bigint PRIMARY KEY)';
  EXECUTE format('ALTER TABLE engine._tx_sentinel_staging OWNER TO %I', owner_role);
  EXECUTE format('ALTER TABLE engine._tx_sentinel_hist    OWNER TO %I', owner_role);
  FOREACH r IN ARRAY writer_roles LOOP
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON engine._tx_sentinel_staging TO %I', r);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON engine._tx_sentinel_hist    TO %I', r);
  END LOOP;

  -- [7] STAGING FLOW — commit-only triggers (SECURITY INVOKER so they can use pg_temp)
  EXECUTE $FN$
    CREATE OR REPLACE FUNCTION engine.trg_collect_staging_stmt()
    RETURNS trigger
    SECURITY INVOKER
    SET search_path = pg_temp, engine, pg_catalog
    LANGUAGE plpgsql
    AS $$
    BEGIN
      CREATE TEMP TABLE IF NOT EXISTS pg_temp.pending_finalize(
        forecast_id uuid PRIMARY KEY
      ) ON COMMIT PRESERVE ROWS;

      INSERT INTO pg_temp.pending_finalize(forecast_id)
      SELECT DISTINCT forecast_id
      FROM new_batch
      WHERE forecast_id IS NOT NULL
      ON CONFLICT DO NOTHING;

      INSERT INTO engine._tx_sentinel_staging(txid)
      VALUES (txid_current())
      ON CONFLICT DO NOTHING;

      RETURN NULL;
    END;
    $$;
  FN$;
  EXECUTE format('ALTER FUNCTION engine.trg_collect_staging_stmt() OWNER TO %I', owner_role);

  EXECUTE $FN$
    CREATE OR REPLACE FUNCTION engine.trg_finalize_staging_commit()
    RETURNS trigger
    SECURITY INVOKER
    SET search_path = pg_temp, engine, pg_catalog
    LANGUAGE plpgsql
    AS $$
    DECLARE r record;
    BEGIN
      IF to_regclass('pg_temp.pending_finalize') IS NOT NULL THEN
        FOR r IN SELECT DISTINCT forecast_id FROM pg_temp.pending_finalize LOOP
          PERFORM engine.build_instance_historical(r.forecast_id);
          PERFORM engine.update_forecast_registry(r.forecast_id);
        END LOOP;
        -- Use DELETE instead of TRUNCATE to avoid ownership edge cases with SECURITY settings
        DELETE FROM pg_temp.pending_finalize;
      END IF;

      DELETE FROM engine._tx_sentinel_staging WHERE txid = NEW.txid;
      RETURN NULL;
    END;
    $$;
  FN$;
  EXECUTE format('ALTER FUNCTION engine.trg_finalize_staging_commit() OWNER TO %I', owner_role);

  EXECUTE $TRG$
    CREATE TRIGGER trg_collect_ids_stmt
    AFTER INSERT ON engine.staging_historical
    REFERENCING NEW TABLE AS new_batch
    FOR EACH STATEMENT
    EXECUTE FUNCTION engine.trg_collect_staging_stmt();

    CREATE CONSTRAINT TRIGGER trg_on_staging_commit
    AFTER INSERT ON engine._tx_sentinel_staging
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION engine.trg_finalize_staging_commit();
  TRG$;

  -- [8] HISTORICAL FLOW — commit-only (SECURITY INVOKER; uses pg_temp)
  EXECUTE $FN$
    CREATE OR REPLACE FUNCTION engine.trg_collect_hist_stmt()
    RETURNS trigger
    SECURITY INVOKER
    SET search_path = pg_temp, engine, pg_catalog
    LANGUAGE plpgsql
    AS $$
    BEGIN
      CREATE TEMP TABLE IF NOT EXISTS pg_temp.pending_hist_finalize(
        forecast_id uuid PRIMARY KEY
      ) ON COMMIT PRESERVE ROWS;

      INSERT INTO pg_temp.pending_hist_finalize(forecast_id)
      SELECT DISTINCT forecast_id
      FROM new_hist
      WHERE forecast_id IS NOT NULL
      ON CONFLICT DO NOTHING;

      INSERT INTO engine._tx_sentinel_hist(txid)
      VALUES (txid_current())
      ON CONFLICT DO NOTHING;

      RETURN NULL;
    END;
    $$;
  FN$;
  EXECUTE format('ALTER FUNCTION engine.trg_collect_hist_stmt() OWNER TO %I', owner_role);

  EXECUTE $FN$
    CREATE OR REPLACE FUNCTION engine.trg_finalize_hist_commit()
    RETURNS trigger
    SECURITY INVOKER
    SET search_path = pg_temp, engine, pg_catalog
    LANGUAGE plpgsql
    AS $$
    DECLARE r record;
    BEGIN
      IF to_regclass('pg_temp.pending_hist_finalize') IS NOT NULL THEN
        FOR r IN SELECT DISTINCT forecast_id FROM pg_temp.pending_hist_finalize LOOP
          -- Your heavy builders SHOULD be SECURITY DEFINER (set in their own scripts) so they can create indexes, etc.
          PERFORM engine.delete_staging_for(r.forecast_id);
          PERFORM engine.build_sr_series_s(r.forecast_id);
          PERFORM engine.build_sr_series_sq(r.forecast_id);
          PERFORM engine.build_sr_series_sqm(r.forecast_id);
        END LOOP;
        -- Avoid TRUNCATE to prevent "permission denied for temporary table" under SECURITY DEFINER
        DELETE FROM pg_temp.pending_hist_finalize;
      END IF;

      DELETE FROM engine._tx_sentinel_hist WHERE txid = NEW.txid;
      RETURN NULL;
    END;
    $$;
  FN$;
  EXECUTE format('ALTER FUNCTION engine.trg_finalize_hist_commit() OWNER TO %I', owner_role);

  EXECUTE $TRG$
    CREATE TRIGGER trg_collect_hist_stmt
    AFTER INSERT ON engine.instance_historical
    REFERENCING NEW TABLE AS new_hist
    FOR EACH STATEMENT
    EXECUTE FUNCTION engine.trg_collect_hist_stmt();

    CREATE CONSTRAINT TRIGGER trg_on_hist_commit
    AFTER INSERT ON engine._tx_sentinel_hist
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION engine.trg_finalize_hist_commit();
  TRG$;

  -- [9] FINAL: ANALYZE so planner has stats from the start
  EXECUTE 'ANALYZE engine.staging_historical';
  EXECUTE 'ANALYZE engine.instance_historical';
END
$$ LANGUAGE plpgsql;

