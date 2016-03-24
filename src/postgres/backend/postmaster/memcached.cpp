// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// setup prepared statements
// GET, SET, ADD, REPLACE, DELETE, CAS, INCR, DECR, APPEND, PREPEND
// FLUSH_ALL, STATS, VERSION, VERBOSITY not implemented
// TODO directly return response from sql as defined in Memcached protocol
// TODO CAS semantic: gets -> unique_cas_token
int setup() {

std::string setup = "
DROP TABLE test;
CREATE TABLE test ( k VARCHAR(40) PRIMARY KEY, v VARCHAR(400) );

DEALLOCATE GET;
PREPARE GET (text) AS
  SELECT d FROM TEST WHERE k = $1;

DEALLOCATE SET;
PREPARE SET (text, text) AS
  INSERT INTO test (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = excluded.v;

DEALLOCATE ADD;
PREPARE ADD (text, text) AS
  INSERT INTO test (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = excluded.v;

DEALLOCATE REPLACE;
PREPARE REPLACE (text, text) AS
  UPDATE test SET v = $2 WHERE k=$1;

DEALLOCATE APPEND;
PREPARE APPEND (text, text) AS
  UPDATE test SET v=CONCAT(v,$2) WHERE k=$1;

DEALLOCATE PREPEND;
PREPARE PREPEND (text, text) AS
  UPDATE test SET v=CONCAT($2,v) WHERE k=$1;

DEALLOCATE INCR;
PREPARE INCR (text) AS
  UPDATE test SET v=CAST(v as int)+1 WHERE k=$1;

DEALLOCATE DECR;
PREPARE DECR (text) AS
  UPDATE test SET v=CAST(v as int)+1 WHERE k=$1;

DEALLOCATE DELETE;
PREPARE DELETE (text) AS
  DELETE FROM test WHERE k=$1;

DEALLOCATE CAS;
PREPARE CAS (text, text, text) AS
  UPDATE test SET v = case when v = '' then 'Y' else 'N' end;
"
}

int get(std::string &key, std::string &value) {
  std::string queryString = "EXECUTE SET (,)";
}

int get(std::string &key, std::string &value) {
}
