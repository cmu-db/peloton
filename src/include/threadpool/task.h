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
#include "event.h"

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
  inline Task(void(*task_ptr)(void*), void* task_arg, void(*task_callback_ptr)(void*), void* task_callback_arg) :
      task_ptr_(task_ptr), task_arg_(task_arg), task_callback_ptr_(task_callback_ptr), task_callback_arg_(task_callback_arg) {};

 private:
  // Functions
  void ExecuteTask();

  // Instance variables
  // Callback function
  void(*task_ptr_)(void *);
  void * task_arg_;
  void(*task_callback_ptr_)(void *);
  void * task_callback_arg_;
};

/**
 * @class TaskQueue
 * @brief A queue for user to submit task and for user to poll tasks
 */
class TaskQueue {
 public:
  inline TaskQueue(const size_t sz) : task_queue_(sz) {};
void ExecutePlanWrapper(void *arg_ptr);
  bool PollTask(std::shared_ptr<Task> &task);
  bool IsEmpty();
  void EnqueueTask(void(*task_ptr)(void*), void* task_arg, void(*task_callback_ptr)(void*), void* task_callback_arg);

 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
};

} // namespace threadpool
} // namespace peloton
