#include "message.pelotonmsg.pb.h"
#include "message.query.pb.h"
//#include "message.tuple.pb.h"
#include <string>
#include <iostream>
#include <fstream>

int main(void) 
{
  peloton::message::pelotonmsg msg;
  peloton::message::query query;
//  peloton::message::tuple tuple;
  std::string type = "select";
  std::string sql = "select * from company";
  query.set_type(type);
  query.set_statement(sql);

  std::string str_query;
  query.SerializeToString(&str_query);

  msg.set_type("SQL");
  msg.set_data(str_query);

  // Write the new address book back to disk. 
  std::fstream output("./log", std::ios::out | std::ios::trunc | std::ios::binary); 

  bool result = msg.SerializeToOstream(&output);
  if (!result) {
    std::cerr << "Failed to write msg" << std::endl;
    return -1;
  }
  
  return 0;
}
