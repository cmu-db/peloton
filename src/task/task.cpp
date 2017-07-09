//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_server.cpp
//
// Identification: src/networking/network_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "task/task.h"

namespace peloton {
namespace task {

void Task::ExecuteTask() {
  this->func_ptr_(this->func_args_);

  std::unique_lock <std::mutex> lock(*this->task_mutex_);
  this->num_worker_--;
  if (*this->num_worker_ == 0) {
    this->condition_variable_->notify_all();
  }
}

void TaskQueue::SubmitTask(Task *task) {
  std::mutex mutex;
  std::unique_lock <std::mutex> lock(mutex);
  std::condition_variable cv;
  task->task_mutex_ = &mutex;
  task->condition_variable_ = &cv;

  task_queue_.Enqueue(task);

  cv.wait(lock);
}

void TaskQueue::SubmitTaskBatch(std::vector < Task * > task_vector) {
  std::mutex mutex;
  std::unique_lock <std::mutex> lock(mutex);
  std::condition_variable cv;
  int task_countDown = task_vector.size();

  for (Task *task: task_vector) {
    task->task_mutex_ = &mutex;
    task->condition_variable_ = &cv;
    task->num_worker_ = &task_countDown;
    task_queue_.Enqueue(task);
  }

  cv.wait(lock);
}

bool TaskQueue::PollTask(Task *task) {
  return task_queue_.Dequeue(task);
}

bool TaskQueue::IsEmpty() {
  return task_queue_.IsEmpty();
}

}
}