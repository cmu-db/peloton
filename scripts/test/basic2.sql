-- create the table
-- 2 columns, 1 primary key

drop table if exists foo;
create table foo(id integer, year integer);
-- create index on foo (id); -- failed, why?

-- load in the data

insert into foo values(1, 100);
insert into foo values(1, 200); -- should fail
insert into foo values(2, 200);
insert into foo values(3, 300);
insert into foo values(4, 400);
insert into foo values(5, 400);
insert into foo values(5, 500); -- should fail

select * from foo;

-- select

select * from foo where id < 3;

select * from foo where year > 200;

-- delete

delete from foo where year = 200;
select * from foo;


-- -- update

update foo set year = 3000 where id = 3;
select * from foo;

update foo set year = 1000 where year = 100; 
select * from foo;

update foo set id = 3 where year = 1000; -- should fail
select * from foo;

update foo set id= 10 where year = 1000;
select * from foo;

-- insert again

insert into foo values (2, 2000);
select * from foo;

insert into foo values (4, 4000); -- should fail
select * from foo;

insert into foo values (1, 1000);
select * from foo;

