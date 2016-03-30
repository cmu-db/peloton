drop table if exists VACATION;
drop table if exists VACATION;

create table VACATION (
	k integer primary key,
	destination varchar(30),
	arrival timestamp,
  departure date
);

insert into VACATION values (1, 'Fort Meyers', '2013-02-16 20:38:40', '2013-02-18');
insert into VACATION values (2, 'Tokyo','2013-04-01 13:24:06', '2013-04-18');
insert into VACATION values (3, 'Seoul', '2014-03-26 05:30:01', '2014-03-29');
insert into VACATION values (4, 'Chengdu', '2014-07-20 08:10:10', '2014-08-20');
insert into VACATION values (5, 'Taipei', '2015-03-20 07:52:19', '2015-04-01');
insert into VACATION values (6, 'Hakushu distillery', '2015-06-07 08:00:00', '2015-12-01');
insert into VACATION values (7, 'Aso-san', '2015-07-12 18:30:00', '2015-11-12');

select * from VACATION;
select extract(year from arrival) from vacation;
select date_part('year', arrival) from vacation;
select extract(month from arrival) from vacation;
select date_part('month', arrival) from vacation;
select extract(day from arrival) from vacation;
select date_part('day', arrival) from vacation;
select extract(hour from arrival) from vacation;
select date_part('hour', arrival) from vacation;
select extract(minute from arrival) from vacation;
select date_part('minute', arrival) from vacation;
select extract(second from arrival) from vacation;
select date_part('second', arrival) from vacation;
select extract(doy from arrival) from vacation;
select date_part('doy', arrival) from vacation;
select extract(dow from arrival) from vacation;
select date_part('dow', arrival) from vacation;
select extract(weekday from arrival) from vacation;
select date_part('weekday', arrival) from vacation;
select extract(woy from arrival) from vacation;
select date_part('woy', arrival) from vacation;
select extract(quarter from arrival) from vacation;
select date_part('quarter', arrival) from vacation;

select * from VACATION where departure < '2013-04-18';
select * from VACATION where departure <= '2013-04-18';
select * from VACATION where departure > '2014-03-29';
select * from VACATION where departure >= '2014-03-29';
select * from VACATION where departure = '2015-04-01';
