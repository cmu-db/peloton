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
#include "concurrency/transaction_manager_factory.h"
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "common/dedicated_thread_task.h"
#include "common/logger.h"
#include "catalog/catalog.h"
#include "kj/debug.h"
#include "peloton/capnp/peloton_service.capnp.h"

namespace peloton {
namespace network {
class PelotonRpcServerImpl final : public PelotonService::Server {
 protected:
  kj::Promise<void> createIndex(CreateIndexContext) override {
    LOG_DEBUG("Received rpc to create index");
    return kj::READY_NOW;
  }
};


class PelotonRpcHandlerTask : public DedicatedThreadTask {
 public:
  explicit PelotonRpcHandlerTask(std::string address) : address_(address) {}

  void Terminate() override {
    // TODO(tianyu): Not implemented. See:
    // https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_.c_str());
    LOG_DEBUG("Server listening on %s", address_.c_str());
    kj::NEVER_DONE.wait(server.getWaitScope());
  }

 private:
  std::string address_;
};
}  // namespace network
}  // namespace peloton
