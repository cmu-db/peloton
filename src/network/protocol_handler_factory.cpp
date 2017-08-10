//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_handler_factory.cpp
//
// Identification: src/include/network/protocol_handler_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "network/protocol_handler_factory.h"
#include "network/postgres_protocol_handler.h"

namespace peloton {
namespace network {
  std::unique_ptr<ProtocolHandler>
  ProtocolHandlerFactory::CreateProtocolHandler(ProtocolHandlerType type) {

    switch (type) {
      case ProtocolHandlerType::Postgres: {
        return
            std::unique_ptr<PostgresProtocolHandler>(
                new PostgresProtocolHandler());
      }
      default:
        return nullptr;
    }
}
}
}

