// SETUP
// GET, SET, ADD, REPLACE, DELETE, CAS, FLUSH_ALL, INCR, DECR, APPEND, PREPEND, STATS

// TODO find way to interact with postgres / peloton?

SET
insert into test (k, d) values ('$key', '$value') on conflict (k) do update set d = excluded.d;

GET
select d from test where k = '$key';