//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tbb_scheduler.h
//
// Identification: src/backend/scheduler/tbb_scheduler.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/scheduler/abstract_scheduler.h"
#include "backend/common/logger.h"

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
    root = new (tbb::task::allocate_root()) tbb::empty_task;
    root->increment_ref_count();
  }

  ~TBBSchedulerState() {
    LOG_TRACE("Destroying root task \n");
    root->set_ref_count(0);
    root->destroy(*root);
    LOG_TRACE("Destroyed root task \n");
  }
};

//===--------------------------------------------------------------------===//
// Scheduler
//===--------------------------------------------------------------------===//

class TBBScheduler : public AbstractScheduler {
 public:
  TBBScheduler();
  ~TBBScheduler();

  // add task to queue
  void Run(handler function_pointer, void *args,
           TaskPriorityType priority = TASK_PRIORTY_TYPE_NORMAL);

  // wait for all tasks
  void Wait();

 private:
  tbb::task_scheduler_init init;

  TBBSchedulerState *state = nullptr;
};

}  // namespace scheduler
}  // namespace peloton
