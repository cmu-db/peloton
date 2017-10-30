//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task.h
//
// Identification: src/threadpool/task.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

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
  Task(void (*task_ptr)(void *), void *task_arg,
       void (*callback_ptr)(void *), void *callback_arg) :
      task_ptr_(task_ptr), task_arg_(task_arg),
      callback_ptr_(callback_ptr), callback_arg_(callback_arg) {};

 private:
  // Run
  void Run() {
    task_ptr_(task_arg_);

    // then call the TaskCallBack
    callback_ptr_(callback_arg_);
  }

  // Instance variables
  void (*task_ptr_)(void *);
  void *task_arg_;

  // Callback function
  void (*callback_ptr_)(void *);
  void *callback_arg_;
};

}  // namespace threadpool
}  // namespace peloton
