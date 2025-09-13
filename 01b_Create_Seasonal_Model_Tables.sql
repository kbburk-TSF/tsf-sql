IMPORT FOREIGN SCHEMA seasonal_matrix
LIMIT TO (me_s_mr30)
FROM SERVER seasonal_matrix_srv
INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
LIMIT TO (mo_s)
FROM SERVER seasonal_matrix_srv
INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
LIMIT TO (wd_md)
FROM SERVER seasonal_matrix_srv
INTO engine;

IMPORT FOREIGN SCHEMA seasonal_matrix
LIMIT TO (mo_sf)
FROM SERVER seasonal_matrix_srv
INTO engine;