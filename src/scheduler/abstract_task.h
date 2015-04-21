/*-------------------------------------------------------------------------
 *
 * task.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/task.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "tbb/tbb.h"

#include <iostream>

namespace nstore {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Abstract Task
//===--------------------------------------------------------------------===//

class AbstractTask : public tbb::task {

 public:
  AbstractTask(void (*task)(void*), void *args)
 : task(task),
   args(args) {

    // Get a task id
    task_id = catalog::Manager::GetNextOid();

  }

  tbb::task* execute() {

    std::cout << "Starting task \n";
    (*task)(args);
    std::cout << "Stopping task \n";

    return nullptr;
  }

  oid_t GetTaskId() {
    return task_id;
  }

 private:
  oid_t task_id;

  void (*task)(void*);
  void *args;
};


} // namespace scheduler
} // namespace nstore
