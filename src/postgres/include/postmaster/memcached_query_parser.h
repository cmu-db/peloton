
#ifndef MEMCACHED_QUERY_PARSER_H
#define MEMCACHED_QUERY_PARSER_H

#include <string>
#include <cstring>
#include <vector>
#include <algorithm>

#include "postmaster/memcached.h"

namespace peloton {
namespace memcached {

class QueryParser {
private:
  //MemcachedSocket* mcsocket;
  std::string memcached_query;
  int op_type;
public:
  int getOpType();
  std::string parseQuery();
//  QueryParser(std::string query, MemcachedSocket* mc_sock) {
  QueryParser(std::string query) {
      //mcsocket=mc_sock;
    memcached_query = query;
  };

  ~QueryParser() {};
}; // end Query parser class

} // end memcached namespace
} // end peloton namespace

#endif