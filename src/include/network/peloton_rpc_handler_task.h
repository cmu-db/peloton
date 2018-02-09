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
#include "common/dedicated_thread_task.h"

namespace peloton {
namespace network {
class PelotonRpcServerImpl final : public PelotonService::Server {
 protected:
  kj::Promise<void> createIndex(CreateIndexContext) override {
    // TODO(tianyu) Write actual index code
    LOG_DEBUG("Received rpc to create index");
    return kj::READY_NOW;
  }
};

class PelotonRpcHandlerTask : public DedicatedThreadTask  {
 public:
  PelotonRpcHandlerTask(const char *address): address_(address) {}

  void Terminate() override {
    // TODO(tianyu): Write when we implement tuning
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_);
    LOG_DEBUG("Server listening on %s", address_);
    kj::NEVER_DONE.wait(server.getWaitScope());
  }

 private:
  const char *address_;
};
}
}
