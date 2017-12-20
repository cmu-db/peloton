//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.cpp
//
// Identification: src/threadpool/worker_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/worker_pool.h"

namespace peloton {
namespace threadpool {

void WorkerFunc(std::atomic_bool *should_shutdown, TaskQueue *task_queue) {
  constexpr auto kMinPauseTime = std::chrono::microseconds(1);
  constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);

  auto pause_time = kMinPauseTime;
  while (!should_shutdown->load() || !task_queue->IsEmpty()) {
    std::function<void()> task;
    if (!task_queue->Dequeue(task)) {
      // Polling with exponential backoff
      std::this_thread::sleep_for(pause_time);
      pause_time = std::min(pause_time * 2, kMaxPauseTime);
    } else {
      task();
      pause_time = kMinPauseTime;
    }
  }
}

}  // namespace threadpool
}  // namespace peloton
