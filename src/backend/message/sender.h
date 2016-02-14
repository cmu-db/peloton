#include "message.pelotonmsg.pb.h"
#include "message.query.pb.h"
//#include "message.tuple.pb.h"
#include <string>
#include <iostream>
#include <fstream>

namespace peloton {
namespace message {

class Sender {
 public:

  /** @brief SendMessage function nanomsg implementation */
  int SendMessage(int socket, const void *buffer, size_t length, int flags);

};

}  // end of message namespace
}  // end of peloton namespace
