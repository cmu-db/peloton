//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mono_queue_pool.h
//
// Identification: test/threadpool/mono_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "worker_pool.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace threadpool {

/**
 * @brief Wrapper class for single queue and single pool.
 * One should use this if possible.
 */
class MonoQueuePool {
 public:
  MonoQueuePool()
      : task_queue_(settings::SettingsManager::GetInt(
            settings::SettingId::mono_queue_pool_num_tasks)),
        worker_pool_(settings::SettingsManager::GetInt(
            settings::SettingId::mono_queue_pool_num_workers), &task_queue_),
        is_running_(false) {}

  ~MonoQueuePool() {
    if (is_running_) {
      Shutdown();
    }
  }

  bool IsRunning() const { return is_running_; }

  size_t NumWorkers() const { return worker_pool_.NumWorkers(); }

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
    static MonoQueuePool mono_queue_pool;
    return mono_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool worker_pool_;
  bool is_running_;
};

}  // namespace threadpool
}  // namespace peloton
