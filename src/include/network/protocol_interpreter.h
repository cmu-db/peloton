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
#include <functional>
#include "network/network_types.h"
#include "network/network_io_utils.h"

namespace peloton {
namespace network {

class ProtocolInterpreter {
 public:
  virtual Transition Process(std::shared_ptr<ReadBuffer> in,
                             std::shared_ptr<WriteQueue> out,
                             CallbackFunc callback) = 0;

  // TODO(Tianyu): Do we really need this crap?
  virtual void GetResult(std::shared_ptr<WriteQueue> out) = 0;
};

} // namespace network
} // namespace peloton