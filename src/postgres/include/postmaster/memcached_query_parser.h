

#include <string>
#include <cstring>
#include <algorithm>


namespace peloton {
namespace memcached {

class QueryParser {
private:
  std::string memcached_query;
  int op_type;
public:
  int getOpType(); // denotes the optype corresponding to the query
  std::string parseQuery(); // parses the command
  QueryParser(std::string query) {
    memcached_query = query;
  };

  ~QueryParser() {};
}; // end Query parser class

} // end memcached namespace
} // end peloton namespace

