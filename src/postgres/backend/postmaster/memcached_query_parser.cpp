//
// Created by siddharth on 17/3/16.
//
#include "postgres/backend/postmaster/memcached_query_parser.h"

namespace peloton {
namespace memcached {

  void QueryParser::parseQuery(){
    //set <key> <flags> <exptime> <bytes> [noreply]\r\n<value>
    //add <key> <flags> <exptime> <bytes> [noreply]\r\n<value>
    //replace <key> <flags> <exptime> <bytes> [noreply]\r\n<value>
    //delete <key> [<time>] [noreply]
    //get <key>
    //multiget

    std::string new_line_del (" ");
    std::size_t found = memcached_query.find(new_line_del);
    if (found!=std::string::npos)
    {
      std::string command = memcached_query.substr(0, found);
      if(command == "get"){
        
      }
      else if(command == "delete"){

      }
      else if(command == "set" || command == "add" || command == "replace" ){

      }
    }else {
      //INVALID CASE
    }
  }


} // end memcached namespace
} // end peloton namespace