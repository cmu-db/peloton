// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// setup prepared statements
// GET, SET, ADD, REPLACE, DELETE, CAS, INCR, DECR, APPEND, PREPEND
// FLUSH_ALL, STATS, VERSION, VERBOSITY not implemented
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
PREPARE INCR (text, text) AS

DEALLOCATE DECR;
PREPARE DECR (text, text) AS

DEALLOCATE DELETE;
PREPARE DELETE (text, text) AS

DEALLOCATE CAS;
PREPARE CAS (text, text, text) AS
"
}

int get(std::string &key, std::string &value) {
  std::string queryString = "EXECUTE SET (,)";
}

int get(std::string &key, std::string &value) {
}
