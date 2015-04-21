/*-------------------------------------------------------------------------
 *
 * scheduler.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/scheduler.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>

#include "scheduler/scheduler.h"
#include "tbb/task_scheduler_init.h"

namespace nstore {
namespace scheduler {

void Scheduler::Init() {

  auto num_cores = tbb::task_scheduler_init::automatic;
  tbb_scheduler = new tbb::task_scheduler_init(num_cores);
  assert(tbb_scheduler != nullptr);

}

void Scheduler::Execute(__attribute__((unused)) AbstractTask *task){

}

} // namespace scheduler
} // namespace nstore

