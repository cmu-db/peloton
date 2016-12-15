-- WARNING: DO NOT MODIFY THIS FILE
-- psql tests use the exact output FROM this file for CI

-- create the table
-- 2 columns, 1 primary key

DROP TABLE IF EXISTS foo;
CREATE TABLE foo(id integer PRIMARY KEY, year integer);
-- create index on foo (id); -- failed, why?

-- load in the data

INSERT INTO foo VALUES(1, 100);
INSERT INTO foo VALUES(1, 200); -- should fail
INSERT INTO foo VALUES(2, 200);
INSERT INTO foo VALUES(3, 300);
INSERT INTO foo VALUES(4, 400);
INSERT INTO foo VALUES(5, 400);
INSERT INTO foo VALUES(5, 500); -- should fail

SELECT id, year FROM foo ORDER BY id;

-- SELECT

SELECT id, year FROM foo WHERE id < 3 ORDER BY id;

SELECT id, year FROM foo WHERE year > 200 ORDER BY id;

-- DELETE

DELETE FROM foo WHERE year = 200;
SELECT id, year FROM foo ORDER BY id;


-- -- UPDATE

UPDATE foo SET year = 3000 WHERE id = 3;
SELECT id, year FROM foo ORDER BY id;

UPDATE foo SET year = 1000 WHERE year = 100; 
SELECT id, year FROM foo ORDER BY id;

UPDATE foo SET id = 3 WHERE year = 1000; -- should fail
SELECT id, year FROM foo ORDER BY id;

UPDATE foo SET id= 10 WHERE year = 1000;
SELECT id, year FROM foo ORDER BY id;

-- insert again

INSERT INTO foo VALUES (2, 2000);
SELECT id, year FROM foo ORDER BY id;

INSERT INTO foo VALUES (4, 4000); -- should fail
SELECT id, year FROM foo ORDER BY id;

INSERT INTO foo VALUES (1, 1000);
SELECT id, year FROM foo ORDER BY id;

