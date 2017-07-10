//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task.h
//
// Identification: src/networking/network_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <condition_variable>
#include <vector>
#include <memory>

#include "container/lock_free_queue.h"

namespace peloton {
namespace task {

class Task {
  friend class TaskQueue;

 public:
  inline Task() {};
  inline Task(void(*func_ptr)(std::vector<void*>),
              std::vector<void*> &func_args) :
      func_ptr_(func_ptr), func_args_(func_args) {};

  void ExecuteTask();

 private:
  void(*func_ptr_)(std::vector<void *>);
  std::vector<void*>func_args_;

  std::mutex *task_mutex_;
  std::condition_variable *condition_variable_;
  int *num_worker_;
};

class TaskQueue {
 public:
  void SubmitTask(std::shared_ptr<Task> task);
  void SubmitTaskBatch(std::vector<std::shared_ptr<Task>>& task_vector);
  bool PollTask(std::shared_ptr<Task> &task);
  bool IsEmpty();
 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
};

}
}
