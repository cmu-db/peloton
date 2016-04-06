//
// Created by siddharth on 17/3/16.
//
#include <string>
#include <cstring>
#include <vector>
#include <algorithm>

namespace peloton {
namespace memcached {

class QueryParser {
private:
  std::string memcached_query;

public:
  void parseQuery();
  QueryParser(std::string query) {
    memcached_query = query;
  };

  ~QueryParser() {};
}; // end Query parser class

} // end memcached namespace
} // end peloton namespace

