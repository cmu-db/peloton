//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dedicated_thread_task.h
//
// Identification: src/include/common/dedicated_thread_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
/**
 * @brief Interface for a task to be run on a dedicated thread
 *
 * A dedicated thread is a long running thread that fulfills some system
 * function running at all times. An example of this would be threads in
 * the worker thread pool or the GC thread.
 */
class DedicatedThreadTask {
 public:
  /**
   * Send a termination signal to the dedicated thread.
   *
   * The thread must then wrap up and exit from its Run function. The
   * termination is guaranteed to be communicated to the owner
   */
  virtual void Terminate() = 0;

  /**
   * Executes the dedicated thread. It is assumed that the thread doesn't exit
   * until terminate is explicitly called.
   */
  virtual void RunTask() = 0;
};
}  // namespace peloton