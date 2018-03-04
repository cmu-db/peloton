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
  virtual void RunJob(BrainEnvironment &env) { printf("foo"); };
};

class Brain {
 public:
  // TODO(tianyu): Add necessary parameters to initialize the brain's resources
  Brain() : scheduler_(0) {}

  inline void RegisterJob(const struct timeval *period,
                          std::string name,
                          BrainJob &job) {
    auto callback = [&](int, short, void *env) {
      job.RunJob(*reinterpret_cast<BrainEnvironment *>(env));
    };
    job_handles_[name] =
        scheduler_.RegisterPeriodicEvent(period, callback, &env);
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
  // TODO(tianyu): May need to have multiple env instead of shared
  BrainEnvironment env;
};
}
}