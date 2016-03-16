drop table if exists foocase;
create table foocase(a integer, b integer, c float, d text);

-- load in data
insert into foocase values (0, 11, 1.1, '#0');
insert into foocase values (1, 12, 2.1, '#1');
insert into foocase values (2, 12, 2.2, '#2');
insert into foocase values (3, 11, 1.2, '#3');
insert into foocase values (4, 12, 2.3, '#4');
insert into foocase values (5, 11, 1.3, '#5');

select case a when 0 then 'zero' end
from foocase;

select case a when 0 then 'zero'
              when 1 then 'one'
              else 'others' end
from foocase;

select case when a = 0 then 'zero' end
from foocase;

select case when a = 0 then 'zero'
            when a = 1 then 'one'
              else 'others' end
from foocase;
