//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire_protocol.h
//
// Identification: src/include/network/wire_protocol.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <memory>
#include "network/marshal.h"

namespace peloton {
namespace network {

class WireProtocol {
 public:
  // TODO(Tianyu): What the hell is this thread_id thingy
  virtual Transition Process(ReadBuffer &in,
                             WriteBuffer &out,
                             size_t thread_id) = 0;

};

} // namespace network
} // namespace peloton