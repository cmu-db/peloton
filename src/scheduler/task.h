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
// Task
//===--------------------------------------------------------------------===//

class Task : public tbb::task {

 public:
  Task(void (*function_pointer)(void*), void *args)
 : function_pointer(function_pointer),
   args(args) {

    // Get a task id
    task_id = catalog::Manager::GetInstance().GetNextOid();

  }

  tbb::task* execute() {

    std::cout << "Starting task \n";
    (*function_pointer)(args);
    std::cout << "Stopping task \n";

    return nullptr;
  }

  oid_t GetTaskId() {
    return task_id;
  }

 private:
  oid_t task_id;

  void (*function_pointer)(void*);
  void *args;
};


} // namespace scheduler
} // namespace nstore
