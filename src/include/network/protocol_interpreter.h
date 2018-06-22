//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_interpreter.h
//
// Identification: src/include/network/protocol_interpreter.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <memory>
#include "network/network_types.h"
#include "network/netork_io_utils.h"

namespace peloton {
namespace network {

class ProtocolInterpreter {
 public:
  // TODO(Tianyu): What the hell is this thread_id thingy
  virtual Transition Process(std::shared_ptr<ReadBuffer> &in,
                             WriteQueue &out,
                             size_t thread_id) = 0;

};

} // namespace network
} // namespace peloton