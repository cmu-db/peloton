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

#define MIN_PAUSE_TIME 1
#define MAX_PAUSE_TIME 1000

namespace peloton {
namespace threadpool {

void Worker::Start(TaskQueue *task_queue) {
  shutdown_thread_ = false;
  worker_thread_ = std::thread(Worker::Execute, this, task_queue);
}

void Worker::Execute(Worker *current_thread, TaskQueue *task_queue) {
  size_t time_pause = MIN_PAUSE_TIME;
  std::shared_ptr<Task> task;
  while (!current_thread->shutdown_thread_ || !task_queue->IsEmpty()) {
    // poll the queue
    if (!task_queue->Poll(task)) {
      usleep(time_pause);
      time_pause = time_pause * 2 < MAX_PAUSE_TIME ?
                       time_pause * 2 : MAX_PAUSE_TIME;
    } else {
      LOG_TRACE("Grabbed one task, now execute it");
      // call the threadpool
      task->Run();
      LOG_TRACE("Finished one task");
      time_pause = MIN_PAUSE_TIME;
    }
  }
}

void Worker::Stop() {
  shutdown_thread_ = true;
  worker_thread_.join();
}

}  // namespace threadpool
}  // namespace peloton
