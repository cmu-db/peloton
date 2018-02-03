//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_handler_factory.h
//
// Identification: src/include/network/protocol_handler_factory.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "network/protocol_handler.h"

// Packet content macros

namespace peloton {

namespace network {

enum class ProtocolHandlerType {
  Postgres,
};

// The factory of ProtocolHandler
class ProtocolHandlerFactory {
 public:
  static std::unique_ptr<ProtocolHandler> CreateProtocolHandler(
      ProtocolHandlerType type, tcop::TrafficCop *trafficCop);
};
}
}
