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

#include "backend/catalog/manager.h"
#include "tbb/tbb.h"
#include "backend/common/types.h"

#include <iostream>

namespace peloton {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Task
//===--------------------------------------------------------------------===//

class Task : public tbb::task {

 public:
  Task(ResultType (*function_pointer)(void*), void *args)
 : function_pointer(function_pointer),
   args(args),
   output(RESULT_TYPE_INVALID){

    // Get a task id
    task_id = catalog::Manager::GetInstance().GetNextOid();

  }

  tbb::task* execute() {

    std::cout << "Starting task \n";
    output = (*function_pointer)(args);
    std::cout << "Stopping task \n";

    return nullptr;
  }

  oid_t GetTaskId() {
    return task_id;
  }

  ResultType GetOuput() {
    return output;
  }

 protected:
  oid_t task_id;

  ResultType (*function_pointer)(void*);
  void *args;
  ResultType output;
};


} // namespace scheduler
} // namespace peloton
