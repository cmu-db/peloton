DROP FUNCTION IF EXISTS add_c(integer, integer);
CREATE FUNCTION add_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS multiply_c(integer, integer);
CREATE FUNCTION multiply_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'multiply' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS add_one_float8_c(float8);
CREATE FUNCTION add_one_float8_c(float8) RETURNS float8 AS '/usr/local/lib/sample_udf.so', 'add_one_float8' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS makepoint(point, point);
CREATE FUNCTION makepoint(point, point) RETURNS point AS '/usr/local/lib/sample_udf.so', 'makepoint' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS copy_text_c(text);
CREATE FUNCTION copy_text_c(text) RETURNS text AS '/usr/local/lib/sample_udf.so', 'copy_text' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS concat_text_c(text, text);
CREATE FUNCTION concat_text_c(text, text) RETURNS text AS '/usr/local/lib/sample_udf.so', 'concat_text' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS calc_tax_c(float8);
CREATE FUNCTION calc_tax_c(float8) RETURNS float8 AS '/usr/local/lib/sample_udf.so', 'calc_tax_c' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS item_sales_sum_c(int);
CREATE FUNCTION item_sales_sum_c(int) RETURNS float8 AS '/usr/local/lib/spi_udf.so', 'item_sales_sum_c' LANGUAGE C STRICT;

