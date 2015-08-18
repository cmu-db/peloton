-- create the table and secondary index

drop table if exists foo;
create table foo(id1 integer, id2 integer, name text, PRIMARY KEY (name));
-- create index sk_foo on foo (id1, id2, name);

-- load in the data

insert into foo values(1, 100, '#1');
insert into foo values(2, 100, '#2');
insert into foo values(3, 200, '#3');
insert into foo values(4, 500, '#4');
insert into foo values(5, 600, '#5');
insert into foo values(6, 100, '#6');
insert into foo values(7, 200, '#7');
insert into foo values(8, 300, '#8');
insert into foo values(9, 100, '#9');
insert into foo values(10, 200, '#10');
insert into foo values(11, 100, '#11');
insert into foo values(12, 200, '#12');
insert into foo values(13, 300, '#13');
select * from foo;
