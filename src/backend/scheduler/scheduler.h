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

#include "backend/scheduler/task.h"
#include "tbb/task_scheduler_init.h"

namespace peloton {
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
  // singleton
  static Scheduler& GetInstance();

  Scheduler();
  ~Scheduler();

  // add task to queue
  void AddTask(ResultType (*function_pointer)(void*),
               void *args,
               TaskPriorityType priority = TASK_PRIORTY_TYPE_NORMAL);

  // wait for all tasks
  void Wait();

 private:
  tbb::task_scheduler_init init;

  SchedulerState *state = nullptr;
};

} // namespace scheduler
} // namespace peloton
