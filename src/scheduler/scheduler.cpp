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

Scheduler::Scheduler() {
  // set up state
  state = new SchedulerState();
}

Scheduler::~Scheduler() {
  // clean up state
  delete state;
}

void Scheduler::AddTask(void (*task)(void*), void *args) {

  tbb::task& tk = *new(state->root->allocate_child()) AbstractTask(task, args);
  state->root->increment_ref_count();
  state->root->spawn(tk);
  std::cout << "Spawned task \n";

}

void Scheduler::Wait() {
  std::cout << "WAITING for tasks \n";
  state->root->wait_for_all();
  state->root->increment_ref_count();
  std::cout << "End of WAIT \n";
}

} // namespace scheduler
} // namespace nstore

