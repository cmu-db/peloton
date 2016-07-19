-- create the table and secondary index

drop table if exists foo;
create table foo(id1 integer, id2 integer, CONSTRAINT pk_foo PRIMARY KEY (id1, id2));
create index sk_foo on foo (id1);

-- load in the data

insert into foo values(1, 100);
insert into foo values(1, 200);
insert into foo values(1, 300);
insert into foo values(1, 400);
insert into foo values(1, 500);
insert into foo values(2, 100);
insert into foo values(2, 200);
insert into foo values(2, 300);
insert into foo values(3, 100);
insert into foo values(3, 200);
insert into foo values(4, 100);
insert into foo values(4, 200);
insert into foo values(4, 300);
select * from foo;


-- delete

delete from foo where id1 = 1 and id2 = 100;
delete from foo where id1 = 1 and id2 = 200;
delete from foo where id1 = 1 and id2 = 300;
delete from foo where id1 = 1 and id2 = 400;
delete from foo where id1 = 1 and id2 = 500;
delete from foo where id1 = 2;
delete from foo where id1 = 3;
delete from foo where id1 = 4;
select * from foo;

SELECT pg_sleep(5);

insert into foo values(1, 500);
select * from foo;

