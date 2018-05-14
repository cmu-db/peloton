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
#include <pqxx/pqxx>
#include "settings/setting_id.h"
#include "settings/settings_manager.h"
#include "capnp/ez-rpc.h"
#include "peloton/capnp/peloton_service.capnp.h"
#include "common/notifiable_task.h"

namespace peloton {
namespace brain {

/**
 * Provides an access point to the various resources available to the jobs in
 * the brain, such as RPC and Catalog.
 */
class BrainEnvironment {
 public:
  BrainEnvironment() :
      rpc_client_{
          settings::SettingsManager::GetString(settings::SettingId::peloton_rpc_address)},
      sql_connection_{
          settings::SettingsManager::GetString(settings::SettingId::peloton_address)} {}

  inline capnp::EzRpcClient &GetPelotonClient() {
    return rpc_client_;
  }

  inline pqxx::result ExecuteQuery(const std::string &query) {
    pqxx::work w(sql_connection_);
    pqxx::result result = w.exec(query);
    w.commit();
    return result;
  }

 private:
  capnp::EzRpcClient rpc_client_;
  // TODO(tianyu): eventually change this into rpc
  pqxx::connection sql_connection_;
};

/**
 * Interface that represents a piece of task to be run on Brain. To use this
 * abstract class, extend it with a concrete class and fill in the method
 * OnJobInvocation.
 */
class BrainJob {
 public:
  explicit BrainJob(BrainEnvironment *env) : env_(env) {}

  virtual ~BrainJob() = default;

  // This is separate from the user-defined OnJobInvocation to allow for better
  // interfacing with the libevent API.
  /**
   * Invokes this job to be run. The brain framework will call this method.
   *
   */
  inline void Invoke() { OnJobInvocation(env_); }
  // TODO(tianyu): Extend this interface for richer behavior
  /**
   * Executed as the main body of the job, filled in by the user. Use the
   * provided BrainEnvironment for interaction with Brain's resources.
   */
  virtual void OnJobInvocation(BrainEnvironment *) = 0;
 private:
  BrainEnvironment *env_;
};

/**
 * Simple implementation of a BrainJob.
 */
class SimpleBrainJob : public BrainJob {
 public:
  explicit SimpleBrainJob(BrainEnvironment *env,
                          std::function<void(BrainEnvironment *)> task)
      : BrainJob(env), task_(std::move(task)) {}
  inline void OnJobInvocation(BrainEnvironment *env) override { task_(env); }
 private:
  std::function<void(BrainEnvironment *)> task_;
};

/**
 * Main running component of the brain. Events can be registered on this event
 * loop and once Run is called, it will invoke handlers every specified time
 * interval
 */
class Brain {
 public:
  // TODO(tianyu): Add necessary parameters to initialize the brain's resources
  Brain() : scheduler_(0) {}

  ~Brain() {
    for (auto &entry : jobs_)
      delete entry.second;
  }

  /**
   * Registers a Job of type BrainJob (given as template parameter) to be run
   * periodically on Brain.
   *
   * @tparam BrainJob The typename of the brain job being submitted, must subclass BrainJob
   * @tparam Args Arguments to pass to the BrainJob constructor, in addition to BrainEnvironment
   * @param period The time period between each run of the job
   * @param name name of the job
   * @param args arguments to BrainJob constructor
   */
  template<typename BrainJob, typename... Args>
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

  /**
   * Executes the main eventloop on the brain. Tasks will begin executing periodically.
   * Does not return unless there is an exception, or the loop is terminated.
   */
  inline void Run() { scheduler_.EventLoop(); }

  /**
   * Terminate the main event loop.
   */
  inline void Terminate() { scheduler_.ExitLoop(); }

 private:
  // Main Event loop
  NotifiableTask scheduler_;
  // collection of all the jobs registered
  std::unordered_map<std::string, BrainJob *> jobs_;
  // mapping of jobs to their event structs (used to trigger the job)
  std::unordered_map<std::string, struct event *> job_handles_;
  // Shared environment for all the tasks
  BrainEnvironment env_;
};
} // namespace brain
} // namespace peloton
