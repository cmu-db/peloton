//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_rpc_handler_task.h
//
// Identification: src/include/network/peloton_rpc_handler_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "common/dedicated_thread_task.h"
#include "common/logger.h"
#include "kj/debug.h"
#include "peloton/capnp/peloton_service.capnp.h"

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

class PelotonRpcHandlerTask : public DedicatedThreadTask {
 public:
  explicit PelotonRpcHandlerTask(const char *address) : address_(address) {}

  void Terminate() override {
    // TODO(tianyu): Not implemented. See:
    // https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_);
    LOG_DEBUG("Server listening on %s", address_);
    kj::NEVER_DONE.wait(server.getWaitScope());
  }

 private:
  const char *address_;
};
}  // namespace network
}  // namespace peloton
