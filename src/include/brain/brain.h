//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// brain.h
//
// Identification: src/include/brain/brain.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <unordered_map>
#include <string>
#include <cstdio>
#include <utility>
#include "capnp/ez-rpc.h"
#include "peloton/capnp/peloton_service.capnp.h"
#include "common/notifiable_task.h"

namespace peloton {
namespace brain {

class BrainEnvironment {
  // TODO(tianyu): provide interface for accessing various resources of the brain,
  // such as network connection to a peloton engine.
};

class BrainJob {
 public:
  // TODO(tianyu): Extend this interface for richer interaction
  virtual void RunJob(BrainEnvironment *) = 0;
};

class ExampleBrainJob: public BrainJob {
 public:
   void RunJob(BrainEnvironment *) override {
     // TODO(tianyu): Replace with real address
     capnp::EzRpcClient client("localhost:15445");
     PelotonService::Client peloton_service = client.getMain<PelotonService>();
     auto request = peloton_service.createIndexRequest();
     request.getRequest().setIndexKeys(42);
     auto response = request.send().wait(client.getWaitScope());
   }
};

class Brain {
 public:
  // TODO(tianyu): Add necessary parameters to initialize the brain's resources
  Brain() : scheduler_(0) {}

  inline void RegisterJob(const struct timeval *period,
                          std::string name,
                          BrainJob &job) {
    auto callback = [](int, short, void *pair) {
      auto *job_env_pair = reinterpret_cast<std::pair<BrainJob *, BrainEnvironment *> *>(pair);
      job_env_pair->first->RunJob(job_env_pair->second);
    };
    // TODO(tianyu) Deal with this memory
    auto *pair = new std::pair<BrainJob *, BrainEnvironment *>(&job, &env_);
    job_handles_[name] =
        scheduler_.RegisterPeriodicEvent(period, callback, pair);
  }

  inline void Run() {
    scheduler_.EventLoop();
  }

  inline void Terminate() {
    scheduler_.ExitLoop();
  }

 private:
  NotifiableTask scheduler_;
  std::unordered_map<std::string, struct event *> job_handles_;
  BrainEnvironment env_;
};
}
}