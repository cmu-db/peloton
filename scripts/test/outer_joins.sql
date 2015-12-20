-- create the test tables
DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

create table A(id INT PRIMARY KEY, data TEXT);
create table B(id INT PRIMARY KEY, data TEXT);

-- load in the data
INSERT INTO A VALUES(0, 'Ming says Hello World 0');
INSERT INTO B VALUES(0, 'Joy says World Hello 0');


-- nested loop join left outer join
SELECT A.* FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT B.* FROM B LEFT OUTER JOIN A ON A.id != B.id;
SELECT A.id, B.id FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.id, B.data FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.id, A.id, B.data, B.data FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.*, B.* FROM A LEFT OUTER JOIN B ON A.id != B.id;

-- nested loop join with right outer join
SELECT A.* FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT B.* FROM B RIGHT OUTER JOIN A ON A.id != B.id;
SELECT A.id, B.id FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.id, B.data FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.id, A.id, B.data, B.data FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.*, B.* FROM A RIGHT OUTER JOIN B ON A.id != B.id;

INSERT INTO A VALUES(1, 'Ming says Hello World 1');
INSERT INTO A VALUES(2, 'Ming says Hello World 2');
INSERT INTO A VALUES(3, 'Ming says Hello World 1');
INSERT INTO B VALUES(1, 'Joy says World Hello 1');
INSERT INTO B VALUES(2, 'Joy says World Hello 2');
INSERT INTO B VALUES(4, 'Joy says World Hello 4');



-- hash join left outer join
SELECT A.* FROM A LEFT OUTER JOIN B ON A.id = B.id;
SELECT B.* FROM B LEFT OUTER JOIN A ON A.id = B.id;
SELECT A.id, B.id FROM A LEFT OUTER JOIN B ON A.id = B.id;
SELECT A.id, B.data FROM A LEFT OUTER JOIN B ON A.id = B.id;
SELECT A.id, A.id, B.data, B.data FROM A LEFT OUTER JOIN B ON A.id = B.id;
SELECT A.*, B.* FROM A LEFT OUTER JOIN B ON A.id = B.id;

-- hash join with right outer join
SELECT A.* FROM A RIGHT OUTER JOIN B ON A.id = B.id;
SELECT B.* FROM B RIGHT OUTER JOIN A ON A.id = B.id;
SELECT A.id, B.id FROM A RIGHT OUTER JOIN B ON A.id = B.id;
SELECT A.id, B.data FROM A RIGHT OUTER JOIN B ON A.id = B.id;
SELECT A.id, A.id, B.data, B.data FROM A RIGHT OUTER JOIN B ON A.id = B.id;
SELECT A.*, B.* FROM A RIGHT OUTER JOIN B ON A.id = B.id;

-- hash join with full outer join
SELECT A.* FROM A FULL OUTER JOIN B ON A.id = B.id;
SELECT B.* FROM B FULL OUTER JOIN A ON A.id = B.id;
SELECT A.id, B.id FROM A FULL OUTER JOIN B ON A.id = B.id;
SELECT A.id, B.data FROM A FULL OUTER JOIN B ON A.id = B.id;
SELECT A.id, A.id, B.data, B.data FROM A FULL OUTER JOIN B ON A.id = B.id;
SELECT A.*, B.* FROM A FULL OUTER JOIN B ON A.id = B.id;


