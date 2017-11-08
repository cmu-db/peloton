//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task.cpp
//
// Identification: src/threadpool/task.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/logger.h>
#include "threadpool/log_task.h"

namespace peloton {
namespace threadpool {

void LogTask::ExecuteTask() {
  task_ptr_(task_arg_);
  // then call the TaskCallBack
  task_callback_ptr_(task_callback_arg_);
}

// Current thread would be blocked until the call back function finishes.
void LogTaskQueue::EnqueueTask(void (*task_ptr)(void*), void* task_arg,
                               void (*task_callback_ptr)(void*),
                               void* task_callback_arg) {
  std::shared_ptr<LogTask> task = std::make_shared<LogTask>(
      task_ptr, task_arg, task_callback_ptr, task_callback_arg);
  task_queue_.Enqueue(task);
}

bool LogTaskQueue::PollTask(std::shared_ptr<LogTask>& task) {
  return task_queue_.Dequeue(task);
}

bool LogTaskQueue::IsEmpty() { return task_queue_.IsEmpty(); }

}  // namespace threadpool
}  // namespace peloton
