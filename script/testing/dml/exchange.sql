-- WARNING: DO NOT MODIFY THIS FILE
-- psql tests use the exact output from this file for CI

-- create the table
-- 2 columns, 1 primary key

drop table if exists foo;
create table foo(id integer PRIMARY KEY, year integer);
-- create index on foo (id); -- failed, why?

-- load in the data

insert into foo values(1, 100);
insert into foo values(2, 200);
insert into foo values(3, 300);
insert into foo values(4, 400);
insert into foo values(5, 500);
insert into foo values(6, 600);
insert into foo values(7, 700);
insert into foo values(8, 800);
insert into foo values(9, 900);
insert into foo values(10, 1000);
insert into foo values(11, 1100);
insert into foo values(12, 1200);
insert into foo values(13, 1300);
insert into foo values(14, 1400);
insert into foo values(15, 1500);
insert into foo values(16, 1600);
insert into foo values(17, 1700);
insert into foo values(18, 1800);
insert into foo values(19, 1900);
insert into foo values(20, 2000);
insert into foo values(21, 2100);
insert into foo values(22, 2200);
insert into foo values(23, 2300);
insert into foo values(24, 2400);
insert into foo values(25, 2500);
insert into foo values(26, 2600);
insert into foo values(27, 2700);
insert into foo values(28, 2800);
insert into foo values(29, 2900);
insert into foo values(30, 3000);
insert into foo values(31, 3100);
insert into foo values(32, 3200);
insert into foo values(33, 3300);
insert into foo values(34, 3400);
insert into foo values(35, 3500);
insert into foo values(36, 3600);
insert into foo values(37, 3700);
insert into foo values(38, 3800);
insert into foo values(39, 3900);
insert into foo values(40, 4000);
insert into foo values(41, 4100);
insert into foo values(42, 4200);
insert into foo values(43, 4300);
insert into foo values(44, 4400);
insert into foo values(45, 4500);
insert into foo values(46, 4600);
insert into foo values(47, 4700);
insert into foo values(48, 4800);
insert into foo values(49, 4900);
insert into foo values(50, 5000);
insert into foo values(51, 5100);
insert into foo values(52, 5200);
insert into foo values(53, 5300);
insert into foo values(54, 5400);
insert into foo values(55, 5500);
insert into foo values(56, 5600);
insert into foo values(57, 5700);
insert into foo values(58, 5800);
insert into foo values(59, 5900);
insert into foo values(60, 6000);

select * from foo;