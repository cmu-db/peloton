//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.h
//
// Identification: src/include/threadpool/mono_queue_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "settings/settings_manager.h"
#include "threadpool/worker_pool.h"

namespace peloton {
namespace threadpool {

/**
 * @brief Wrapper class for single queue and single pool.
 */
class MonoQueuePool {
 public:
  MonoQueuePool(const std::string &name, uint32_t task_queue_size,
                uint32_t worker_pool_size);

  ~MonoQueuePool();

  void Startup();

  void Shutdown();

  void SubmitTask(std::function<void()> func);

  size_t NumWorkers() const { return worker_pool_.NumWorkers(); }

  /// Instances for various components
  static MonoQueuePool &GetInstance();
  // TODO(Tianyu): Rename to (Brain)QueryHistoryLog or something
  static MonoQueuePool &GetBrainInstance();
  static MonoQueuePool &GetExecutionInstance();

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation
///
////////////////////////////////////////////////////////////////////////////////

inline MonoQueuePool::MonoQueuePool(const std::string &name,
                                    uint32_t task_queue_size,
                                    uint32_t worker_pool_size)
    : task_queue_(task_queue_size),
      worker_pool_(name, worker_pool_size, task_queue_),
      is_running_(false) {}

inline MonoQueuePool::~MonoQueuePool() {
  if (is_running_) {
    Shutdown();
  }
}

inline void MonoQueuePool::Startup() {
  worker_pool_.Startup();
  is_running_ = true;
}

inline void MonoQueuePool::Shutdown() {
  worker_pool_.Shutdown();
  is_running_ = false;
}

inline void MonoQueuePool::SubmitTask(std::function<void()> func) {
  if (!is_running_) {
    Startup();
  }
  task_queue_.Enqueue(std::move(func));
}

inline MonoQueuePool &MonoQueuePool::GetInstance() {
  int32_t task_queue_size = settings::SettingsManager::GetInt(
      settings::SettingId::monoqueue_task_queue_size);
  int32_t worker_pool_size = settings::SettingsManager::GetInt(
      settings::SettingId::monoqueue_worker_pool_size);

  PL_ASSERT(task_queue_size > 0);
  PL_ASSERT(worker_pool_size > 0);

  std::string name = "main-pool";

  static MonoQueuePool mono_queue_pool(name,
                                       static_cast<uint32_t>(task_queue_size),
                                       static_cast<uint32_t>(worker_pool_size));
  return mono_queue_pool;
}

inline MonoQueuePool &MonoQueuePool::GetBrainInstance() {
  int32_t task_queue_size = settings::SettingsManager::GetInt(
      settings::SettingId::brain_task_queue_size);
  int32_t worker_pool_size = settings::SettingsManager::GetInt(
      settings::SettingId::brain_worker_pool_size);

  PL_ASSERT(task_queue_size > 0);
  PL_ASSERT(worker_pool_size > 0);

  std::string name = "brain-pool";

  static MonoQueuePool brain_queue_pool(
      name, static_cast<uint32_t>(task_queue_size),
      static_cast<uint32_t>(worker_pool_size));
  return brain_queue_pool;
}

inline MonoQueuePool &MonoQueuePool::GetExecutionInstance(){
  int32_t task_queue_size = settings::SettingsManager::GetInt(
      settings::SettingId::monoqueue_task_queue_size);
  int32_t worker_pool_size = settings::SettingsManager::GetInt(
      settings::SettingId::monoqueue_worker_pool_size);

  PL_ASSERT(task_queue_size > 0);
  PL_ASSERT(worker_pool_size > 0);

  std::string name = "executor-pool";

  static MonoQueuePool brain_queue_pool(
      name, static_cast<uint32_t>(task_queue_size),
      static_cast<uint32_t>(worker_pool_size));
  return brain_queue_pool;
}

}  // namespace threadpool
}  // namespace peloton
