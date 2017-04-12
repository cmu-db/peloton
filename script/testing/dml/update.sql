-- create the table
create table foo (a integer, b integer);

-- load in the data
insert into foo values (1, 200);
insert into foo values (2, 150);
insert into foo values (3, 400);

-- update with WHERE
update foo set b = 100 where a = 2;

-- update without WHERE
update foo set b = 100;
