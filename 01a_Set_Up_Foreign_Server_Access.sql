-- =====================================================================
-- Create Connections to Seasonal Models (Foreign Tables)
--  * Establishes Foreign Server
--  * DO NOT RUN — MUST BE UPDATED/CUSTOMIZED
-- =====================================================================



CREATE SERVER IF NOT EXISTS seasonal_matrix_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host    'ep-winter-dew-adnmg5hw-pooler.c-2.us-east-1.aws.neon.tech',
    dbname  'neondb',
    port    '5432',
    sslmode 'require'
  );


CREATE USER MAPPING IF NOT EXISTS
FOR aq_engine_owner
SERVER seasonal_matrix_srv
OPTIONS (
  user 'neondb_owner',
  password 'npg_YlT60kjRFICA'
);


CREATE USER MAPPING IF NOT EXISTS
FOR neondb_owner
SERVER seasonal_matrix_srv
OPTIONS (
  user 'neondb_owner',
  password 'npg_YlT60kjRFICA'
);



