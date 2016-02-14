#include "message.pelotonmsg.pb.h"
#include "message.query.pb.h"
#include "message.tuple.pb.h"
#include <string>
#include <iostream>
#include <fstream>


using namespace std;

 void ListMsg(const peloton::message::pelotonmsg & msg) { 
  string type = msg.type();
    
  cout << type << endl;
    
  if ( type == "SQL") {
      peloton::message::query query;
      string str_query = msg.data();
      bool result = query.ParseFromString(str_query);
      if (result == true) {
      string query_type = query.type();
      string query_statement = query.statement();

      cout << "query_type: " << query_type << endl;
      cout << "query_statement: " << query_statement << endl;
      } else {
        cout << "Failed to parse query";
      }
   } else  {
      cout << "Not query" << endl;
   }
  
  cout << msg.type() << endl;
   
  cout << msg.data() << endl; 
 } 
 
 int main(int argc, char* argv[]) { 

  peloton::message::pelotonmsg msg1; 
 
  { 
    fstream input("./log", ios::in | ios::binary); 
    if (!msg1.ParseFromIstream(&input)) { 
      cerr << "Failed to parse address book." << endl; 
      return -1; 
    } 
  } 
 
  ListMsg(msg1); 

}
