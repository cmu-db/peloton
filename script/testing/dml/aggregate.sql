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
select sum(c), avg(c), b from fooagg group by b;

select a, b, sum(c) from fooagg group by a, b;

select b, max(a) from fooagg group by b;
select b, max(a) from fooagg group by b having max(a) > 4;

select b, max(c) from fooagg group by b;
select b, max(c) from fooagg group by b having max(c) > 2;

-- plain aggregate
select sum(a) from fooagg;
select count(b) as cnt, count(distinct b) as cntdistinct from fooagg;

-- simple distinct
insert into fooagg values (6, 11, 1.1, '#6');
insert into fooagg values (7, 11, 1.2, '#7');

select * from fooagg;

select b, count(c), count(distinct c) from fooagg group by b;
select b, sum(c), sum(distinct c) from fooagg group by b;


