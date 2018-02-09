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

// Dumb code does not have a no throw destructor, preventing smart-pointer use
// This a temporary work around, see:
// https://github.com/capnproto/capnproto/issues/553

template <typename T>
struct DestructorCatcher {
  T value;
  template <typename... Params>
  DestructorCatcher(Params&&... params) : value(kj::fwd<Params>(params)...) {}
  ~DestructorCatcher() noexcept try {} catch (kj::Exception& e) {
    LOG_DEBUG("Unexpected destructor throw");
  }
  T *operator->() { return &value; }
};

class PelotonRpcHandlerTask : public DedicatedThreadTask {
 public:
  PelotonRpcHandlerTask(const char *address)
      : address_(address),
        termination_trigger_(kj::newPromiseAndFulfiller<void>()) {}\

  void Terminate() override {
    termination_trigger_->fulfiller->fulfill();
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_);
    LOG_DEBUG("Server listening on %s", address_);
    termination_trigger_->promise.wait(server.getWaitScope());
  }

 private:
  using PromiseFulfillerPair = DestructorCatcher<kj::PromiseFulfillerPair<void>>;
  const char *address_;
  PromiseFulfillerPair termination_trigger_;
};
}  // namespace network
}  // namespace peloton
