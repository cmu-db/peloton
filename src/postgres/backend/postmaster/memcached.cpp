// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// setup prepared statements
// GET, SET, ADD, REPLACE, DELETE, CAS, INCR, DECR, APPEND, PREPEND
// FLUSH_ALL, STATS, VERSION, VERBOSITY not implemented
// TODO directly return response from sql as defined in Memcached protocol
// TODO CAS semantic: gets -> unique_cas_token
int setup() {

// "
// CREATE FUNCTION test() RETURNS varchar AS $$
//     select * from test
//     RETURNING 'ab';
// $$ LANGUAGE SQL;
// "

std::string setup = "
DROP TABLE test;
CREATE TABLE test ( key VARCHAR(200) PRIMARY KEY, value VARCHAR(2048), flag smallint, size smallint );

DEALLOCATE GET;
PREPARE GET (text) AS
  SELECT key, flag, size, value FROM TEST WHERE key = $1;

DEALLOCATE SET;
PREPARE SET (text, text, smallint, smallint) AS
  INSERT INTO test (key, value, flag, size) VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO UPDATE SET value = excluded.value, flag = excluded.flag, size = excluded.size;

DEALLOCATE ADD;
PREPARE ADD (text, text, smallint, smallint) AS
  INSERT INTO test (key, value, flag, size) VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO UPDATE SET value = excluded.value, flag = excluded.flag, size = excluded.size;

DEALLOCATE REPLACE;
PREPARE REPLACE (text, text, smallint, smallint) AS
  UPDATE test SET value = $2, flag = $3, size = $4 WHERE key=$1;
"

std::string not_needed = 
"
DEALLOCATE APPEND;
PREPARE APPEND (text, text) AS
  UPDATE test SET value=CONCAT(value,$2) WHERE key=$1;

DEALLOCATE PREPEND;
PREPARE PREPEND (text, text) AS
  UPDATE test SET value=CONCAT($2,value) WHERE key=$1;

DEALLOCATE INCR;
PREPARE INCR (text) AS
  UPDATE test SET value=CAST(value as int)+1 WHERE key=$1;

DEALLOCATE DECR;
PREPARE DECR (text) AS
  UPDATE test SET value=CAST(value as int)+1 WHERE key=$1;

DEALLOCATE DELETE;
PREPARE DELETE (text) AS
  DELETE FROM test WHERE key=$1;

DEALLOCATE CAS;
PREPARE CAS (text, text, text) AS
  UPDATE test SET value = case when v = '' then 'Y' else 'N' end;
"
}

int get(std::string &key, std::string &value) {
  std::string queryString = "EXECUTE SET (,)";
}

int get(std::string &key, std::string &value) {
}
