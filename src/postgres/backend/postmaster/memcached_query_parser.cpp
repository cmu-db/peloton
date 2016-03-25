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
    std::string key;
    std::string value;
    int flags; //unsigned int?
    int exptime;
    int bytes;

    auto space_del (" ");
    auto found = memcached_query.find(space_del);
    if (found!=std::string::npos)
    {
      auto command = memcached_query.substr(0, found);
      if(command == "get"){
        key = memcached_query.substr(found+1);
      }
      else if(command == "delete"){
        //TODO: handle non-empty time field later
        auto last_found = memcached_query.find_last_of(space_del);
        if(last_found == found){
          key = memcached_query.substr(last_found+1);

        }
//        else{
//          auto temp_string = memcached_query.substr(last_found+1);
//          if(temp_string == "noreply"){
//            //TODO: No response from server expected return right away
//          }
//        }
      }
      else if(command == "set" || command == "add" || command == "replace" ){
        //key cannot have spaces or /r/n
        //TODO: Try boost

        char *token = std::strtok(&memcached_query[0], " ");
        token = std::strtok(NULL, " ");
        flags = atoi(token);
        token = std::strtok(NULL, " ");
        exptime = atoi(token);
        token = std::strtok(NULL, " ");
        bytes = atoi(token);
        //TODO: Handle noreply

        auto new_line_del ("\r\n");
        found = memcached_query.find(new_line_del);
        value = memcached_query.substr(found+2);

      }
    }else {
      //INVALID CASE
    }
  }


} // end memcached namespace
} // end peloton namespace