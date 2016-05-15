-- create the test tables
INSERT INTO A1 VALUES(1, 0);
INSERT INTO A1 VALUES(1, 1);
INSERT INTO A1 VALUES(1, 2);
INSERT INTO A1 VALUES(1, 3);
INSERT INTO A1 VALUES(1, 4);
INSERT INTO A1 VALUES(1, 5);
INSERT INTO A1 VALUES(1, 6);
INSERT INTO A1 VALUES(1, 7);
INSERT INTO A1 VALUES(1, 8);
INSERT INTO A1 VALUES(1, 9);

-- updates
UPDATE A1 set value = 11 where value = 1;
UPDATE A1 set value = 21 where value = 2;
UPDATE A1 set value = 31 where value = 3;

-- deletes
DELETE FROM A1 where value = 4;
DELETE FROM A1 where value = 5;
DELETE FROM A1 where value = 6;

INSERT INTO B1 VALUES(0, 10);
INSERT INTO B1 VALUES(0, 11);
INSERT INTO B1 VALUES(0, 12);
INSERT INTO B1 VALUES(0, 13);
INSERT INTO B1 VALUES(0, 14);
INSERT INTO B1 VALUES(0, 15);
INSERT INTO B1 VALUES(0, 16);
INSERT INTO B1 VALUES(0, 17);

-- updates
UPDATE B1 set value = 20 where value = 10;
UPDATE B1 set value = 21 where value = 11;
UPDATE B1 set value = 22 where value = 12;

-- deletes
DELETE FROM B1 where value = 14;
DELETE FROM B1 where value = 15;
DELETE FROM B1 where value = 16;



