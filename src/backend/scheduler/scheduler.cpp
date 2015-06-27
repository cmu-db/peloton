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

#include "backend/scheduler/scheduler.h"
#include "tbb/task_scheduler_init.h"
#include "backend/common/exception.h"
#include <string>

namespace peloton {
namespace scheduler {


Scheduler& Scheduler::GetInstance() {
  static Scheduler scheduler;
  return scheduler;
}

Scheduler::Scheduler() :
            init(tbb::task_scheduler_init::default_num_threads()) {

  // set up state
  state = new SchedulerState();
}

Scheduler::~Scheduler() {

  // stop scheduler
  init.terminate();

  // clean up state
  delete state;
}

void Scheduler::AddTask(ResultType (*function_pointer)(void*),
                        void *args,
                        TaskPriorityType priority) {

  Task *task = new(state->root->allocate_child()) Task(function_pointer, args);
  state->root->increment_ref_count();

  // Enqueue task with appropriate priority
  switch(priority) {
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
} // namespace peloton

