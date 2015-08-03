-- create the table and secondary index

drop table if exists foo;
create table foo(id1 integer, id2 integer, CONSTRAINT pk_foo PRIMARY KEY (id1, id2));
create index on foo (id1);

-- load in the data

insert into foo values(1, 1);
insert into foo values(1, 1);
insert into foo values(1, 2);
insert into foo values(1, 5);
insert into foo values(1, 6);
insert into foo values(2, 1);
insert into foo values(2, 2);
insert into foo values(2, 3);
insert into foo values(3, 1);
insert into foo values(3, 2);
insert into foo values(4, 1);
insert into foo values(4, 2);
insert into foo values(4, 3);
select * from foo;

-- delete

delete from foo where id2 = 2;
select * from foo;

-- update

update foo set id2 = 5 where id2 = 1; 
select * from foo;

update foo set id1 = 3 where id2 = 5; 
select * from foo;

