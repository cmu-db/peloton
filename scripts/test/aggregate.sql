drop table if exists fooagg;
create table fooagg(a integer, b integer, c float, d text);

-- load in data
insert into fooagg values (0, 11, 1.1, '#0');
insert into fooagg values (1, 12, 2.1, '#1');
insert into fooagg values (2, 12, 2.2, '#2');
insert into fooagg values (3, 11, 1.2, '#3');
insert into fooagg values (4, 12, 2.3, '#4');
insert into fooagg values (5, 11, 1.3, '#5');

-- select
select * from fooagg;

-- simple
select sum(c), b from fooagg group by b;

select a, b, sum(c) from fooagg group by a, b;

select b, max(a) from fooagg group by b;
select b, max(a) from fooagg group by b having max(a) > 4;

select b, max(c) from fooagg group by b;
select b, max(c) from fooagg group by b having max(c) > 2;
