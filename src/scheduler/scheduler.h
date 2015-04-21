/*-------------------------------------------------------------------------
 *
 * scheduler.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/scheduler.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "task.h"

namespace nstore {
namespace scheduler {

class SchedulerState {
  friend class Scheduler;

 private:
  tbb::task *root;

 public:

  SchedulerState() {
    // Start root task
    root = new(tbb::task::allocate_root()) tbb::empty_task;
    root->increment_ref_count();
  }

  ~SchedulerState() {
    std::cout << "Destroying root task \n";
    root->set_ref_count(0);
    root->destroy(*root);
    std::cout << "Destroyed root task \n";
  }

};


//===--------------------------------------------------------------------===//
// Scheduler
//===--------------------------------------------------------------------===//

class Scheduler {

 public:
  Scheduler();
  ~Scheduler();

  // add task to queue
  void AddTask(void (*task)(void*), void *args);

  // wait for all tasks
  void Wait();

 private:
  SchedulerState* state;
};

} // namespace scheduler
} // namespace nstore
