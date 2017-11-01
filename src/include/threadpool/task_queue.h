//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_queue.h
//
// Identification: src/threadpool/task_queue.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "container/lock_free_queue.h"
#include "threadpool/task.h"

namespace peloton {
namespace threadpool {

/**
 * @class TaskQueue
 * @brief A queue for user to submit task and for user to poll tasks
 */
class TaskQueue {
 public:
  TaskQueue(const size_t size) : task_queue_(size) {};

  bool Poll(std::shared_ptr<Task> &task) {
    return task_queue_.Dequeue(task);
  }

  bool IsEmpty() { return task_queue_.IsEmpty(); }

  void Enqueue(void (*task_ptr)(void *), void *task_arg,
               void (*callback_ptr)(void *), void *callback_arg) {
    std::shared_ptr<Task> task =
        std::make_shared<Task>(task_ptr, task_arg, callback_ptr, callback_arg);

    task_queue_.Enqueue(task);
  }

 private:
  peloton::LockFreeQueue<std::shared_ptr<Task>> task_queue_;
};

}  // namespace threadpool
}  // namespace peloton
