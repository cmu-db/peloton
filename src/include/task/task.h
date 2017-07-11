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

class Result {
 public:
  int getStatusCode();
  std::vector<void *> getResult();

 private:
  int status_code;
  std::vector<void *> result_;
};

class Task {
  friend class TaskQueue;
  friend class Result;

 public:
  inline Task() {};
  inline Task(void(*func_ptr)(void*),
              void* func_args) :
      func_ptr_(func_ptr), func_args_(func_args) {};

  void ExecuteTask();
  Result* getResult();

 private:
  void(*func_ptr_)(void *);
  void* func_args_;

  std::mutex *task_mutex_;
  std::condition_variable *condition_variable_;
  int *num_worker_;
  Result *result_;
};

class TaskQueue {
 public:
  inline TaskQueue(const size_t sz) : task_queue_(sz) {};
  void SubmitTask(std::shared_ptr<Task> task);
  void SubmitTaskBatch(std::vector<std::shared_ptr<Task>>& task_vector);
  bool PollTask(std::shared_ptr<Task> &task);
  bool IsEmpty();
 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
};

}
}
