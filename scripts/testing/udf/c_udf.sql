

DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;


CREATE TABLE A(x integer, y integer);
CREATE TABLE B(test integer);


INSERT INTO A VALUES (1, 0);
INSERT INTO A VALUES (2, 3);
INSERT INTO A VALUES (-1, -10);
INSERT INTO A VALUES (11, 99);
INSERT INTO A VALUES (-2, 5);

INSERT INTO B VALUES (3);
INSERT INTO B VALUES (4);
INSERT INTO B VALUES (10);
INSERT INTO B VALUES (8);

DROP FUNCTION IF EXISTS add_one_c(integer);
DROP FUNCTION IF EXISTS add_c(integer, integer);
DROP FUNCTION IF EXISTS minus_c(integer, integer);
DROP FUNCTION IF EXISTS multiply_c(integer, integer);
DROP FUNCTION IF EXISTS divide_c(integer, integer);
DROP FUNCTION IF EXISTS add_one_float8_c(float8);
DROP FUNCTION IF EXISTS exec_sql(text);

CREATE FUNCTION add_one_c(integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add_one' LANGUAGE C STRICT;
CREATE FUNCTION add_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add' LANGUAGE C STRICT;
CREATE FUNCTION minus_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'minus' LANGUAGE C STRICT;
CREATE FUNCTION multiply_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'multiply' LANGUAGE C STRICT;
CREATE FUNCTION divide_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'divide' LANGUAGE C STRICT;
CREATE FUNCTION add_one_float8_c(float8) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add_one_float8' LANGUAGE C STRICT;


CREATE FUNCTION exec_sql(text) RETURNS integer AS '/usr/local/lib/spi_udf.so', 'execq' LANGUAGE C;
