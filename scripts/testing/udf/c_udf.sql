DROP FUNCTION IF EXISTS add_one_c(integer);
DROP FUNCTION IF EXISTS add_c(integer, integer);
DROP FUNCTION IF EXISTS minus_c(integer, integer);
DROP FUNCTION IF EXISTS multiply_c(integer, integer);
DROP FUNCTION IF EXISTS divide_c(integer, integer);
DROP FUNCTION IF EXISTS add_one_float8_c(float8);
DROP FUNCTION IF EXISTS makepoint(point, point);
DROP FUNCTION IF EXISTS copy_text_c(text);
DROP FUNCTION IF EXISTS concat_text_c(text, text);
DROP FUNCTION IF EXISTS exec_sql(text);

CREATE FUNCTION add_one_c(integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add_one' LANGUAGE C STRICT;
CREATE FUNCTION add_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add' LANGUAGE C STRICT;
CREATE FUNCTION minus_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'minus' LANGUAGE C STRICT;
CREATE FUNCTION multiply_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'multiply' LANGUAGE C STRICT;
CREATE FUNCTION divide_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'divide' LANGUAGE C STRICT;
CREATE FUNCTION add_one_float8_c(float8) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add_one_float8' LANGUAGE C STRICT;
CREATE FUNCTION makepoint(point, point) RETURNS point AS '/usr/local/lib/sample_udf.so', 'makepoint' LANGUAGE C STRICT;
CREATE FUNCTION copy_text_c(text) RETURNS text AS '/usr/local/lib/sample_udf.so', 'copy_text' LANGUAGE C STRICT;
CREATE FUNCTION concat_text_c(text, text) RETURNS text AS '/usr/local/lib/sample_udf.so', 'concat_text' LANGUAGE C STRICT;

CREATE FUNCTION exec_sql(text) RETURNS integer AS '/usr/local/lib/spi_udf.so', 'execq' LANGUAGE C;
