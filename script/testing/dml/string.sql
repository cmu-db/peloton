drop table if exists BLAH

;
drop table if exists blah;

create table blah (
	k integer primary key,
	v varchar(20)
);

insert into BLAH values (1, 'blah1');
insert into BLAH values (2, 'carnegie');
insert into BLAH values (3, 'database');
insert into BLAH values (4, 'computer');
insert into BLAH values (5, 'garbage1');
insert into BLAH values (6, 'garbage2');
insert into BLAH values (7, ' garbage2 ');

select substring(v from 1 for 3) from blah;
select substring(v from 2) from blah;
select substr(v,1,4) from blah;
select substr(v,1) from blah;
select ascii(v) from blah;
select v || v from blah;
select concat(v , v) from blah;
select chr(100+k) from blah;
select char_length(v) from blah;
select character_length(v) from blah;
select octet_length(v) from blah;
select overlay(v placing 'xxx' from 2 for 1) from blah;
select overlay(v placing 'xxx' from 2) from blah;
select position('car' in v) from blah;
select repeat(substring(v from 2 for 3), 2) from blah;
select left(v, 2) from blah;
select right(v, 3) from blah;
select replace(v, 'ar', 'xx') from blah;
select trim(both 'c' from v) from blah;
select btrim(v, 'c') from blah;
select trim(both from v) from blah;
select btrim(v) from blah;
select trim(leading 'c' from v) from blah;
select ltrim(v, 'c') from blah;
select trim(leading from v) from blah;
select ltrim(v) from blah;
select trim(trailing from v) from blah;
select rtrim(v) from blah;
select trim(trailing '1' from v) from blah;
select rtrim(v, '1') from blah;

