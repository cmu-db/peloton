/*-------------------------------------------------------------------------
 *
 * tbb_scheduler.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/tbb_scheduler.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/scheduler/abstract_scheduler.h"
#include "backend/scheduler/tbb_task.h"
#include "tbb/task_scheduler_init.h"

namespace peloton {
namespace scheduler {

class TBBSchedulerState {
  friend class TBBScheduler;

 private:
  tbb::task *root;

 public:

  TBBSchedulerState() {
    // Start root task
    root = new(tbb::task::allocate_root()) tbb::empty_task;
    root->increment_ref_count();
  }

  ~TBBSchedulerState() {
    // Destroy root task
    root->set_ref_count(0);
    root->destroy(*root);
  }

};


//===--------------------------------------------------------------------===//
// TBB Scheduler
//===--------------------------------------------------------------------===//

class TBBScheduler : public AbstractScheduler {

 public:

  TBBScheduler();
  ~TBBScheduler();

  void Run(AbstractTask *task);

  // wait for all tasks
  void Wait();

 private:
  tbb::task_scheduler_init init;

  TBBSchedulerState *state = nullptr;
};

} // namespace scheduler
} // namespace peloton
