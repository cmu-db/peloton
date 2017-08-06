//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task.h
//
// Identification: src/threadpool/task.h
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
namespace threadpool {
/**
 * @class Task
 * @brief Element in threadpool queue that can be execute by workers
 */
class Task {
  friend class TaskQueue; // grant access to sync variables
  friend class Worker; // grant access to ExecuteTask

 public:
  inline Task() {};
  inline Task(void(*func_ptr)(void*), void* func_arg) :
      func_ptr_(func_ptr), func_arg_(func_arg) {};

 private:
  // Functions
  void ExecuteTask();

  // Instance variables
  // Callback function
  void(*func_ptr_)(void *);
  void* func_arg_;
};

/**
 * @class TaskQueue
 * @brief A queue for user to submit task and for user to poll tasks
 */
class TaskQueue {
 public:
  inline TaskQueue(const size_t sz) : task_queue_(sz) {};

  bool PollTask(std::shared_ptr<Task> &task);
  bool IsEmpty();
  void EnqueueTask(void(*func_ptr)(void *), void* func_args);

 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
};

} // namespace threadpool
} // namespace peloton
