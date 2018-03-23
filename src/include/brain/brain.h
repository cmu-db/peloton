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
#include <functional>
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
  friend class Brain;
 public:
  explicit BrainJob(BrainEnvironment *env) : env_(env) {}
  virtual ~BrainJob() = default;
  inline void Invoke() { OnJobInvocation(env_); }
  // TODO(tianyu): Extend this interface for richer interaction
  virtual void OnJobInvocation(BrainEnvironment *) = 0;
 private:
  BrainEnvironment *env_;
};

class SimpleBrainJob : public BrainJob {
 public:
  explicit SimpleBrainJob(BrainEnvironment *env,
                          std::function<void(BrainEnvironment *)> task)
      : BrainJob(env), task_(std::move(task)) {}
  inline void OnJobInvocation(BrainEnvironment *env) override { task_(env); }
 private:
  std::function<void(BrainEnvironment *)> task_;
};

class Brain {
 public:
  // TODO(tianyu): Add necessary parameters to initialize the brain's resources
  Brain() : scheduler_(0) {}
  ~Brain() {
    for (auto entry : jobs_)
      delete entry.second;
    for (auto entry : job_handles_)
      event_free(entry.second);
  }

  template <typename BrainJob, typename... Args>
  inline void RegisterJob(const struct timeval *period,
                          std::string name, Args... args) {
    auto *job = new BrainJob(&env_, args...);
    jobs_[name] = job;
    auto callback = [](int, short, void *arg) {
      reinterpret_cast<BrainJob *>(arg)->Invoke();
    };
    job_handles_[name] =
        scheduler_.RegisterPeriodicEvent(period, callback, job);
  }

  inline void Run() {
    scheduler_.EventLoop();
  }

  inline void Terminate() {
    scheduler_.ExitLoop();
  }

 private:
  NotifiableTask scheduler_;
  std::unordered_map<std::string, BrainJob *> jobs_;
  std::unordered_map<std::string, struct event *> job_handles_;
  BrainEnvironment env_;
};
}
}