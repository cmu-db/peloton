//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.h
//
// Identification: src/include/threadpool/mono_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "settings/settings_manager.h"
#include "worker_pool.h"

namespace peloton {
namespace threadpool {

/**
 * @brief Wrapper class for single queue and single pool.
 * One should use this if possible.
 */
class MonoQueuePool {
 public:
  MonoQueuePool(uint32_t task_queue_size, uint32_t worker_pool_size)
      : task_queue_(task_queue_size),
        worker_pool_(worker_pool_size, &task_queue_),
        is_running_(false) {}

  ~MonoQueuePool() {
    if (is_running_) {
      Shutdown();
    }
  }

  void Startup() {
    worker_pool_.Startup();
    is_running_ = true;
  }

  void Shutdown() {
    worker_pool_.Shutdown();
    is_running_ = false;
  }

  void SubmitTask(std::function<void()> func) {
    if (!is_running_) {
      Startup();
    }
    task_queue_.Enqueue(std::move(func));
  }

  static MonoQueuePool &GetInstance() {
    uint32_t task_queue_size = settings::SettingsManager::GetInt(
        settings::SettingId::monoqueue_task_queue_size);
    uint32_t worker_pool_size = settings::SettingsManager::GetInt(
        settings::SettingId::monoqueue_worker_pool_size);
    static MonoQueuePool mono_queue_pool(task_queue_size, worker_pool_size);
    return mono_queue_pool;
  }

  // TODO(Tianyu): Rename to (Brain)QueryHistoryLog or something
  static MonoQueuePool &GetBrainInstance() {
    uint32_t task_queue_size = settings::SettingsManager::GetInt(
        settings::SettingId::brain_task_queue_size);
    uint32_t worker_pool_size = settings::SettingsManager::GetInt(
        settings::SettingId::brain_worker_pool_size);
    static MonoQueuePool brain_queue_pool(task_queue_size, worker_pool_size);
    return brain_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

}  // namespace threadpool
}  // namespace peloton
