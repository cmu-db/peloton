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
#include "threadpool/task.h"

namespace peloton {
namespace threadpool {

void Task::ExecuteTask() {
  task_ptr_(task_arg_);
  // then call the TaskCallBack
  task_callback_ptr_(task_callback_arg_);
}

// Current thread would be blocked until the call back function finishes.
void TaskQueue::EnqueueTask(void(*task_ptr)(void*), void* task_arg, void(*task_callback_ptr)(void*), void* task_callback_arg){
  std::shared_ptr<Task> task = std::make_shared<Task>(task_ptr, task_arg, task_callback_ptr, task_callback_arg);
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