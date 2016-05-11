DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;
DROP TABLE IF EXISTS C;

CREATE TABLE A(x integer, y integer);
CREATE TABLE B(x numeric, y numeric);
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

DROP FUNCTION IF EXISTS add_plpgsql(integer, integer);
CREATE OR REPLACE FUNCTION add_plpgsql(x integer, y integer) RETURNS integer AS
$$
BEGIN
    RETURN x + y;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS add_one_float_plpgsql(numeric);
CREATE OR REPLACE FUNCTION add_one_float_plpgsql(x numeric) RETURNS numeric AS
$$
BEGIN
    RETURN x + 1.0;
END;
$$ LANGUAGE 'plpgsql';


DROP FUNCTION IF EXISTS multiply_plpgsql(integer, integer);
CREATE OR REPLACE FUNCTION multiply_plpgsql(x integer, y integer) RETURNS integer AS
$$
BEGIN
    RETURN x * y;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS concat_text_plpgsql(text, text);
CREATE OR REPLACE FUNCTION concat_text_plpgsql(x text, y text) RETURNS text AS
$$
BEGIN
    RETURN x || y;
END;
$$ LANGUAGE 'plpgsql';

-- test select x()
SELECT x, y, add_plpgsql(x,y) FROM A;

-- test where clause
SELECT x, y, multiply_plpgsql(x,y) FROM A WHERE multiply_plpgsql(x,y) > 0;

-- test having clause
SELECT A.x, COUNT(*) from A GROUP BY x HAVING COUNT(*) > multiply_plpgsql(-1,-1);

-- test order by
SELECT x, y, multiply_plpgsql(x,y) FROM A ORDER BY multiply_plpgsql(x,y);

-- test group by
SELECT multiply_plpgsql(x,y), count(*) FROM A GROUP BY multiply_plpgsql(x,y);

-- nested function
SELECT multiply_plpgsql(add_plpgsql(x,y), y) from A where multiply_plpgsql(add_plpgsql(x,y), y) > add_plpgsql(multiply_plpgsql(2,3), add_plpgsql(2,1));

-- check float type
SELECT x, y, add_one_float_plpgsql(x), add_one_float_plpgsql(y) from B;

-- check text type
SELECT x, y, concat_text_plpgsql(x, y) from C;
