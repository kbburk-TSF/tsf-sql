-- Wire engine DB to the shared binomial_lut lookup (public.binom_p)
-- Uses your connection: tsf_matrix_app / npg_n0gXFWIESxw2 @ ep-winter-dew-adnmg5hw-pooler.c-2.us-east-1.aws.neon.tech

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS seasonal_binom_srv CASCADE;
CREATE SERVER seasonal_binom_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'ep-winter-dew-adnmg5hw-pooler.c-2.us-east-1.aws.neon.tech',
    port '5432',
    dbname 'binomial_lut',
    sslmode 'require',
    channel_binding 'require'
  );

DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER seasonal_binom_srv;
CREATE USER MAPPING FOR PUBLIC
  SERVER seasonal_binom_srv
  OPTIONS (user 'tsf_matrix_app', password 'npg_n0gXFWIESxw2');

CREATE SCHEMA IF NOT EXISTS binom;

DROP FOREIGN TABLE IF EXISTS binom.binom_p;
IMPORT FOREIGN SCHEMA public
  LIMIT TO (binom_p)
  FROM SERVER seasonal_binom_srv
  INTO binom;

GRANT USAGE ON SCHEMA binom TO PUBLIC;
GRANT SELECT ON binom.binom_p TO PUBLIC;
