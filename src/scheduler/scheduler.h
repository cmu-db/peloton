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

#include "abstract_task.h"

namespace nstore {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Scheduler
//===--------------------------------------------------------------------===//

class Scheduler {
 public:

  void Init();

  void Execute(AbstractTask *task);

 private:
  tbb::task_scheduler_init *tbb_scheduler;

};

} // namespace scheduler
} // namespace nstore
