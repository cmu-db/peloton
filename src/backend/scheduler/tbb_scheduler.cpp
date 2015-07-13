/*-------------------------------------------------------------------------
 *
 * tbb_scheduler.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/tbb_scheduler.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/scheduler/tbb_scheduler.h"
#include "backend/common/exception.h"

#include <cassert>
#include <string>

namespace peloton {
namespace scheduler {

TBBScheduler::TBBScheduler() :
            init(tbb::task_scheduler_init::default_num_threads()) {

  // set up state
  state = new TBBSchedulerState();

}

TBBScheduler::~TBBScheduler() {

  // Stop scheduler
  init.terminate();

  // Clean up state
  delete state;

}

void TBBScheduler::Run(AbstractTask *task) {

  TBBTask *tbb_task = new(state->root->allocate_child()) TBBTask(task->GetTask(), task->GetArgs());
  state->root->increment_ref_count();
  auto priority = tbb_task->GetPriority();

  // Enqueue task with appropriate priority
  switch(priority) {
    case TaskPriorityType::TASK_PRIORTY_TYPE_NORMAL:
      state->root->enqueue(*tbb_task);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_LOW:
      state->root->enqueue(*tbb_task, tbb::priority_low);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_HIGH:
      state->root->enqueue(*tbb_task, tbb::priority_high);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_INVALID:
    default:
      throw SchedulerException("Invalid priority type : " + std::to_string(priority));
      break;
  }

}

void TBBScheduler::Wait() {

  // Wait for all tasks
  state->root->wait_for_all();
  state->root->increment_ref_count();

}

} // namespace scheduler
} // namespace peloton

