-- create the table and secondary index
drop table if exists foo;
create table foo(id1 integer, id2 integer, CONSTRAINT pk_foo PRIMARY KEY (id1, id2));
create index foo_id1 on foo (id1);

-- load in the data

insert into foo values(1, 1);
-- insert into foo values(1, 1);
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

-- use primary index

-- select * from foo where id1 = 1 and id2 = 2;
-- select * from foo where id1 = 1 and id2 > 2;
-- select * from foo where id1 = 1 and id2 >= 2;
-- select * from foo where id1 = 1 and id2 < 2;
-- select * from foo where id1 = 1 and id2 <= 2;

-- use secondary index

-- select * from foo where id1 = 1;
-- select * from foo where id1 > 2;
-- select * from foo where id1 < 3;
-- select * from foo where id1 > 1 and id1 < 4;
-- select * from foo where id1 >= 3;
-- select * from foo where id1 <= 4;

-- delete

-- delete from foo where id1 = 2;

-- use primary index again

-- select * from foo where id1 = 1 and id2 >= 2;

-- update

-- update table foo set id2 = 5 where id1 = 1; 
-- update table foo set id1 = 3 where id1 = 5; 

-- use primary index again

-- select * from foo where id1 = 1 and id2 >= 2;

