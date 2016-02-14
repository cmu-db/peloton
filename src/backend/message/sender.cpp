//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.cpp
//
// Identification: src/backend/message/sender.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/message/sender.h"

namespace peloton {
namespace message {

/**
 *
 */
int Sender::SendMessage(int socket, const void *buffer, size_t length,
                         int flags) {
  std::cout << " SendMsg Called" << socket << buffer << length << flags << std::endl;
  return 0;
}

}
}
