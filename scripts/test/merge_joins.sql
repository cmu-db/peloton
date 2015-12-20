-- creAte the test tABles
DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(id INT PRIMARY KEY, data TEXT);
CREATE TABLE B(id INT PRIMARY KEY, data TEXT);

-- loAd in the data
INSERT INTO A VALUES(0, 'Ming sAys Hello World 0');
INSERT INTO A VALUES(1, 'Ming sAys Hello World 1');
INSERT INTO A VALUES(2, 'Ming sAys Hello World 2');
INSERT INTO A VALUES(3, 'Ming sAys Hello World 3');
INSERT INTO A VALUES(8, 'Ming sAys Hello World 8');
INSERT INTO A VALUES(9, 'Ming sAys Hello World 9');

INSERT INTO B VALUES(0, 'Joy sAys World Hello 0');
INSERT INTO B VALUES(1, 'Joy sAys World Hello 1');
INSERT INTO B VALUES(2, 'Joy sAys World Hello 2');
INSERT INTO B VALUES(3, 'Joy sAys World Hello 3');
INSERT INTO B VALUES(4, 'Joy sAys World Hello 4');
INSERT INTO B VALUES(5, 'Joy sAys World Hello 5');
INSERT INTO B VALUES(6, 'Joy sAys World Hello 6');
INSERT INTO B VALUES(7, 'Joy sAys World Hello 7');

SET ENABLE_NESTLOOP TO FALSE;
SET ENABLE_HASHJOIN TO FALSE;

-- merge join
SELECT * FROM (SELECT * FROM A ORDER BY id) AS AA JOIN (SELECT * FROM B ORDER BY id) AS BB ON AA.id = BB.id;
SELECT * FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;


-- merge join with projections
SELECT A.* FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT B.* FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.*,B.* FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.data FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.id FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.id FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.data FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.id FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;
SELECT A.data FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;

SELECT * FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;

SELECT B.data FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;

SELECT B.id FROM A,B WHERE A.id = B.id ORDER BY A.id, B.id;

-- left outer
SELECT A.* FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, A.id;
SELECT B.* FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.*,B.* FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.data FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.id FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.id FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.data FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data FROM A LEFT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;


-- right outer
SELECT A.* FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT B.* FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.*,B.* FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.data FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.id FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.id FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.data FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data FROM A RIGHT OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;


-- full outer
SELECT A.* FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT B.* FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.*,B.* FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.data FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.id FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id,B.id FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data,B.data FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.id FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;
SELECT A.data FROM A FULL OUTER JOIN B ON A.id = B.id ORDER BY A.id, B.id;

SET ENABLE_NESTLOOP TO TRUE;
SET ENABLE_HASHJOIN TO TRUE;
