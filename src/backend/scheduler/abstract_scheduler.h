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
  virtual ~AbstractScheduler() {};

  // add task to queue
  virtual void AddTask(AbstractTask *task) = 0;

  // wait for execution of all tasks
  virtual void Execute() = 0;

};

} // namespace scheduler
} // namespace peloton
