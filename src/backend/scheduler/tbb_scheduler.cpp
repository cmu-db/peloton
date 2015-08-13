//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tbb_scheduler.cpp
//
// Identification: src/backend/scheduler/tbb_scheduler.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <cassert>

#include "backend/scheduler/tbb_scheduler.h"
#include "backend/common/exception.h"

namespace peloton {
namespace scheduler {

TBBScheduler::TBBScheduler()
    : init(tbb::task_scheduler_init::default_num_threads()) {
  // set up state
  state = new TBBSchedulerState();
  LOG_TRACE("STATE : %p \n", state);
}

TBBScheduler::~TBBScheduler() {
  // stop scheduler
  init.terminate();

  // clean up state
  delete state;
}

void TBBScheduler::Run(handler function_pointer, void *args,
                       TaskPriorityType priority) {
  AbstractTask *task = new (state->root->allocate_child())
      AbstractTask(function_pointer, args, priority);
  assert(task);

  state->root->increment_ref_count();

  // Enqueue task with appropriate priority
  switch (priority) {
    case TaskPriorityType::TASK_PRIORTY_TYPE_NORMAL:
      state->root->enqueue(*task);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_LOW:
      state->root->enqueue(*task, tbb::priority_low);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_HIGH:
      state->root->enqueue(*task, tbb::priority_high);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_INVALID:
    default:
      throw SchedulerException("Invalid priority type : " +
                               std::to_string(priority));
      break;
  }

  LOG_TRACE("Enqueued task \n");
}

void TBBScheduler::Wait() {
  LOG_TRACE("WAITING for tasks \n");
  state->root->wait_for_all();
  state->root->increment_ref_count();
  LOG_TRACE("End of WAIT \n");
}

}  // namespace scheduler
}  // namespace peloton
