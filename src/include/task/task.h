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
  friend class Result;

 public:
  inline Task() {};
  inline Task(void(*func_ptr)(void*), void* func_args) :
      func_ptr_(func_ptr), func_args_(func_args) {};

  void ExecuteTask();
  void ExecuteTaskSync();
  void ExecuteTaskAsync();

 private:
  void(*func_ptr_)(void *);
  void* func_args_;

  bool is_sync = false;
  std::mutex *task_mutex_;
  std::condition_variable *condition_variable_;
  int *num_worker_;
};


class TaskQueue {

 public:
  inline TaskQueue(const size_t sz) : task_queue_(sz) {};
  void SubmitTaskBatch(std::vector<std::shared_ptr<Task>>& task_vector);
  bool PollTask(std::shared_ptr<Task> &task);
  bool IsEmpty();
  void SubmitSync(void(*func_ptr)(void *),
                             void* func_args);
  void SubmitAsync(void(*func_ptr)(void *),
                              void* func_args);

 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
  void SubmitTask(std::shared_ptr<Task> task);
};

}
}
