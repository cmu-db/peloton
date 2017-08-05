//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task.cpp
//
// Identification: src/threadpool/task.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/logger.h>
#include "threadpool/task.h"

namespace peloton {
namespace threadpool {

void Task::ExecuteTask() {
  this->func_ptr_(this->func_arg_);
}

// Current thread would be blocked until the call back function finishes.
void TaskQueue::SubmitTask(void(*func_ptr)(void *), void* func_arg) {
  std::shared_ptr<Task> task = std::make_shared<Task>(func_ptr, func_arg);
  task_queue_.Enqueue(task);
}

bool TaskQueue::PollTask(std::shared_ptr<Task> &task) {
  return task_queue_.Dequeue(task);
}

bool TaskQueue::IsEmpty() {
  return task_queue_.IsEmpty();
}

} // namespace threadpool
} // namespace peloton