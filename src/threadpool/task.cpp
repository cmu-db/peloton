//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// threadpool.cpp
//
// Identification: src/networking/network_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/logger.h>
#include "threadpool/task.h"

namespace peloton {
namespace threadpool {

void Task::ExecuteTask() {
  LOG_DEBUG("Task grab by some worker");
  if (this->is_sync) {
    this->ExecuteTaskSync();
  }
  else {
    this->ExecuteTaskAsync();
  }
  LOG_DEBUG("Now task is done");
}

void Task::ExecuteTaskSync() {
  this->func_ptr_(this->func_arg_);
  std::unique_lock <std::mutex> lock(*this->task_mutex_);
  (*num_worker_)--;
  if ((*num_worker_) == 0) {
    this->condition_variable_->notify_all();
  }
}

void Task::ExecuteTaskAsync() {
  this->func_ptr_(this->func_arg_);
}

// Current thread would be blocked until the call back function finishes.
void TaskQueue::SubmitSync(void(*func_ptr)(void *), void* func_arg) {
  std::shared_ptr<Task> task = std::make_shared<Task>(func_ptr, func_arg);
  std::mutex mutex;
  std::unique_lock <std::mutex> lock(mutex);
  std::condition_variable cv;
  int num_worker = 1;

  task->is_sync = true;
  task->task_mutex_ = &mutex;
  task->condition_variable_ = &cv;
  task->num_worker_ = &num_worker;
  task_queue_.Enqueue(task);
  cv.wait(lock);
}

// Current thread would not be blocked even if
// the call back function is still executing
void TaskQueue::SubmitAsync(void(*func_ptr)(void *), void* func_arg) {
  std::shared_ptr<Task> task = std::make_shared<Task>(func_ptr, func_arg);
  task->is_sync = false;
  task_queue_.Enqueue(task);
}

// TODO: this function was implemented before the need is clear.
// TODO: Not sure if we still want it?
void TaskQueue::SubmitTaskBatch(std::vector<std::shared_ptr<Task>>& task_vector) {
  std::mutex mutex;
  std::unique_lock <std::mutex> lock(mutex);
  std::condition_variable cv;
  int task_countDown = task_vector.size();

  for (std::shared_ptr<Task> task: task_vector) {
    task->task_mutex_ = &mutex;
    task->is_sync = true;
    task->condition_variable_ = &cv;
    task->num_worker_ = &task_countDown;
    task_queue_.Enqueue(task);
  }

  cv.wait(lock);
}

bool TaskQueue::PollTask(std::shared_ptr<Task> &task) {
  return task_queue_.Dequeue(task);
}

bool TaskQueue::IsEmpty() {
  return task_queue_.IsEmpty();
}

}
}