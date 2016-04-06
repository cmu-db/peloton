//
// Created by siddharth on 17/3/16.
//
#include "postmaster/memcached_query_parser.h"

namespace peloton {
namespace memcached {
  //get-0, set-1, add-2, replace-3
  int QueryParser::getOpType() {
    return op_type;
  }
  std::string QueryParser::parseQuery(){
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
    std::string return_string;
    auto space_del (" ");
    auto found = memcached_query.find(space_del);
    if (found!=std::string::npos)
    {
      auto command = memcached_query.substr(0, found);
      if(command == "get"){
        key = memcached_query.substr(found+1);
//        return_string =  "EXECUTE GET ('"+key+"')";
        return_string = "SELECT key, flag, size, value FROM TEST WHERE key = '"+key+"'";
        op_type=0;
        return return_string;
      }
      else if(command == "delete"){
        //TODO: handle non-empty time field later
        auto last_found = memcached_query.find_last_of(space_del);
        if(last_found == found){
          key = memcached_query.substr(last_found+1);
          return_string =  "EXECUTE DELETE ('"+key+"')";
          return return_string;
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
//        printf("%c\n",command[0]);
        //TODO: Handle noreply

        char *token = std::strtok(&memcached_query[0], " ");
        token = std::strtok(NULL, " ");
        key = token;
        token = std::strtok(NULL, " ");
        flags = atoi(token);
        token = std::strtok(NULL, " ");
        exptime = atoi(token);
        token = std::strtok(NULL, " ");
        bytes = atoi(token);
//        printf("key:%s,flags:%d,exptime:%d,bytes:%d\n",key.c_str(),flags,exptime,bytes);
        value="$$$$"; //TODO: Can do better here
//        if(!mcsocket->read_line(value)){
//          return "Failure String";
//        }

        switch(command[0]){
          case 's':{
            op_type=1;
            command = "SET";
            return_string = "INSERT INTO test (key, value, flag, size) VALUES ('"+key+"', '"+value+"', "+std::to_string(flags)+", "+std::to_string(bytes)+") ON CONFLICT (key) DO UPDATE SET value = excluded.value, flag = excluded.flag, size = excluded.size";
            break;
          }
          case 'a':{
            op_type=2;
            command = "ADD";
            return_string = "INSERT INTO test (key, value, flag, size) VALUES ('"+key+"', '"+value+"', "+std::to_string(flags)+", "+std::to_string(bytes)+") ON CONFLICT (key) DO UPDATE SET value = excluded.value, flag = excluded.flag, size = excluded.size";
            break;
          }
          case 'r':{
            op_type=3;
            command = "REPLACE";
            return_string = "UPDATE test SET value = '"+value+"', flag = "+std::to_string(flags)+", size = "+std::to_string(bytes)+" WHERE key='"+key+"'";
            break;
          }
          default:
            return_string="Failed String";
            break;
        }

//        auto new_line_del ("\r\n");
//        found = memcached_query.find(new_line_del);
//        value = memcached_query.substr(found+2);
//        printf("%s\n",value.c_str());

//        return_string = "EXECUTE "+command+" ('"+key+"','"+value+"',"+std::to_string(flags)+","+std::to_string(bytes)+")";

        return return_string;
      }
    }else {

      //INVALID CASE
    }
    op_type=-1;
    return "Failure String";
  }


} // end memcached namespace
} // end peloton namespace