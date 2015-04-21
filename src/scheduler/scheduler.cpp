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
#include "common/exception.h"
#include <string>

namespace nstore {
namespace scheduler {


Scheduler::Scheduler() {
  // set up state
  state = new SchedulerState();
}

Scheduler::~Scheduler() {
  // clean up state
  delete state;
}

void Scheduler::AddTask(void (*function_pointer)(void*), void *args,
                        TaskPriorityType priority) {

  tbb::task *tk = new(state->root->allocate_child()) Task(function_pointer, args);
  state->root->increment_ref_count();

  // Enqueue task with appropriate priority
  switch(priority) {
    case TaskPriorityType::TASK_PRIORTY_TYPE_NORMAL:
      state->root->enqueue(*tk);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_LOW:
      state->root->enqueue(*tk, tbb::priority_low);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_HIGH:
      state->root->enqueue(*tk, tbb::priority_high);
      break;

    case TaskPriorityType::TASK_PRIORTY_TYPE_INVALID:
    default:
      throw SchedulerException("Invalid priority type : " + std::to_string(priority));
      break;
  }

  std::cout << "Enqueued task \n";
}

void Scheduler::Wait() {

  std::cout << "WAITING for tasks \n";
  state->root->wait_for_all();
  state->root->increment_ref_count();
  std::cout << "End of WAIT \n";
}

} // namespace scheduler
} // namespace nstore

