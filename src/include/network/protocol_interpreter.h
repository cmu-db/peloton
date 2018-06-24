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
using callback_func = std::function<void(void)>;

class ProtocolInterpreter {
 public:
  ProtocolInterpreter(size_t thread_id) : thread_id_(thread_id) {}

  virtual Transition Process(std::shared_ptr<ReadBuffer> in,
                             std::shared_ptr<WriteQueue> out,
                             callback_func callback) = 0;

  // TODO(Tianyu): Do we really need this crap?
  virtual void GetResult() = 0;
 protected:
  size_t thread_id_;
};

} // namespace network
} // namespace peloton