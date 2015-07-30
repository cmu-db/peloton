/*-------------------------------------------------------------------------
 *
 * abstract_scheduler.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/abstract_scheduler/abstract_scheduler.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/scheduler/abstract_task.h"

namespace peloton {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Abstract Scheduler
//===--------------------------------------------------------------------===//

class AbstractScheduler {
 public:
  AbstractScheduler() {}
  virtual ~AbstractScheduler(){};

  // run task
  virtual void Run(handler function_pointer, void *args,
                   TaskPriorityType priority = TASK_PRIORTY_TYPE_NORMAL) = 0;

  // wait for execution of all tasks
  virtual void Wait() = 0;
};

}  // namespace scheduler
}  // namespace peloton
