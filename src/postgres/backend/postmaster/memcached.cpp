// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// setup prepared statements
// GET, SET, ADD, REPLACE, DELETE, CAS, INCR, DECR, APPEND, PREPEND
// FLUSH_ALL, STATS, VERSION, VERBOSITY not implemented
int setup() {

std::string setup = "
CREATE TABLE test ( k VARCHAR(40) PRIMARY KEY, d VARCHAR(400) );

DEALLOCATE GET;
PREPARE GET (text) AS
  SELECT d FROM TEST WHERE k = $1;

DEALLOCATE SET;
PREPARE SET (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE ADD;
PREPARE ADD (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE REPLACE;
PREPARE REPLACE (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE APPEND;
PREPARE APPEND (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;
  UPDATE test SET col=CONCAT('$2',col) WHERE key=$1;

DEALLOCATE PREPEND;
PREPARE PREPEND (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE INCR;
PREPARE INCR (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE DECR;
PREPARE DECR (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE DELETE;
PREPARE DELETE (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE CAS;
PREPARE CAS (text, text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;
"
}

int get(std::string &key, std::string &value) {
  std::string queryString = "EXECUTE SET (,)";
}

int get(std::string &key, std::string &value) {
}