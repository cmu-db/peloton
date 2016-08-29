DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;
DROP TABLE IF EXISTS C;

CREATE TABLE A(x integer, y integer);
CREATE TABLE B(x float, y float);
CREATE TABLE C(x text, y text);

INSERT INTO A VALUES (1, 0);
INSERT INTO A VALUES (2, 3);
INSERT INTO A VALUES (-1, -10);
INSERT INTO A VALUES (11, 99);
INSERT INTO A VALUES (3, 2);
INSERT INTO A VALUES (-2, 5);
INSERT INTO A VALUES (9, 5);
INSERT INTO A VALUES (11, 20);
INSERT INTO A VALUES (6, 1);
INSERT INTO A VALUES (9, 121);

INSERT INTO B VALUES (3.213, 4.2132);
INSERT INTO B VALUES (1.522, 2.5425);
INSERT INTO B VALUES (2.321312, 9.3213);
INSERT INTO B VALUES (8.231312, -2.664);

INSERT INTO C VALUES ('ab', 'cd');
INSERT INTO C VALUES ('ef', 'gh');
INSERT INTO C VALUES ('ij', 'kl');

DROP FUNCTION IF EXISTS add_c(integer, integer);
DROP FUNCTION IF EXISTS multiply_c(integer, integer);
DROP FUNCTION IF EXISTS add_one_float8_c(float8);
DROP FUNCTION IF EXISTS concat_text_c(text, text);

CREATE FUNCTION add_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'add' LANGUAGE C STRICT;
CREATE FUNCTION multiply_c(integer, integer) RETURNS integer AS '/usr/local/lib/sample_udf.so', 'multiply' LANGUAGE C STRICT;
CREATE FUNCTION add_one_float8_c(float8) RETURNS float8 AS '/usr/local/lib/sample_udf.so', 'add_one_float8' LANGUAGE C STRICT;
CREATE FUNCTION concat_text_c(text, text) RETURNS text AS '/usr/local/lib/sample_udf.so', 'concat_text' LANGUAGE C STRICT;

-- test select x()
SELECT x, y, add_c(x,y) FROM A;

-- test where clause
SELECT x, y, multiply_c(x,y) FROM A WHERE multiply_c(x,y) > 0;

-- test having clause
SELECT A.x, COUNT(*) from A GROUP BY x HAVING COUNT(*) > multiply_c(-1,-1);

-- test order by
SELECT x, y, multiply_c(x,y) FROM A ORDER BY multiply_c(x,y);

-- test group by
SELECT multiply_c(x,y), count(*) FROM A GROUP BY multiply_c(x,y);

-- nested function
SELECT multiply_c(add_c(x,y), y) from A where multiply_c(add_c(x,y), y) > add_c(multiply_c(2,3), add_c(2,1));

-- check float type
SELECT x, y, add_one_float8_c(x), add_one_float8_c(y) from B;

-- check text type
SELECT x, y, concat_text_c(x, y) from C;
