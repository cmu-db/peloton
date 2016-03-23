// SETUP
// GET, SET, ADD, REPLACE, DELETE, CAS, FLUSH_ALL, INCR, DECR, APPEND, PREPEND, STATS

// TODO find way to interact with postgres / peloton?

// prepared statements
// execute only once
int setup() {
CREATE TABLE test ( k VARCHAR(40) PRIMARY KEY, d VARCHAR(400) );

DEALLOCATE SET;
PREPARE SET (text, text) AS
  INSERT INTO test (k, d) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET d = excluded.d;

DEALLOCATE GET;
PREPARE GET (text) AS
  SELECT d FROM TEST WHERE k = $1;
}

int get(std::string &key, std::string &value) {
  EXECUTE SET (,)
}

int get(std::string &key, std::string &value) {
  EXECUTE SET (,)
}