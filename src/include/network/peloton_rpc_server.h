//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_rpc_server.h
//
// Identification: src/include/network/peloton_rpc_server.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "peloton/capnp/peloton_service.capnp.h"
#include "kj/debug.h"
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "common/logger.h"

namespace peloton {
namespace network {
class PelotonRpcServer final : public PelotonService::Server {
 public:
  PelotonRpcServer() = default;

  void Run(const char *address) {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServer>(), address);
    LOG_DEBUG("Server listening on port %d",
              server.getPort().wait(server.getWaitScope()));
    kj::NEVER_DONE.wait(server.getWaitScope());
  }
 protected:
  kj::Promise<void> createIndex(CreateIndexContext) override {
    LOG_DEBUG("Received rpc to create index");
    return kj::READY_NOW;
  }
};
}
}
