//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker.cpp
//
// Identification: src/threadpool/worker.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include "threadpool/worker.h"
#include "common/logger.h"

// TODO Add comments
#define EMPTY_COUNT_BOUND 10
#define WORKER_PAUSE_TIME 10

namespace peloton {
namespace threadpool {

void Worker::Start(TaskQueue *task_queue) {
  shutdown_thread_ = false;
  worker_thread_ = std::thread(Worker::Execute, this, task_queue);
}

void Worker::Execute(Worker *current_thread, TaskQueue *task_queue) {
  size_t empty_count = 0;
  std::shared_ptr<Task> task;
  while (!current_thread->shutdown_thread_ || !task_queue->IsEmpty()) {
    // poll the queue
    if (!task_queue->Poll(task)) {
      empty_count++;
      if (empty_count == EMPTY_COUNT_BOUND) {
        empty_count = 0;
        usleep(WORKER_PAUSE_TIME);
      }
    } else {
      LOG_TRACE("Grabbed one task, now execute it");
      // call the threadpool
      task->Run();
      LOG_TRACE("Finished one task");
      empty_count = 0;
    }
  }
}

void Worker::Stop() {
  shutdown_thread_ = true;
  worker_thread_.join();
}

}  // namespace threadpool
}  // namespace peloton
