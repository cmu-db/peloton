create table a(id int, value varchar);
insert into a values(1, 'hi');
insert into a values(1, NULL);
select * from a where value is null;
select * from a where value is not null;