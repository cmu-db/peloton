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
#include "backend/common/types.h"

#include <iostream>

namespace peloton {
namespace scheduler {

typedef  ResultType (*handler)(void*);

//===--------------------------------------------------------------------===//
// Abstract Task
//===--------------------------------------------------------------------===//

class AbstractTask {

 public:
  AbstractTask(handler function_pointer, void *args)
 : function_pointer(function_pointer),
   args(args),
   output(RESULT_TYPE_INVALID){

    // Get a task id
    task_id = catalog::Manager::GetInstance().GetNextOid();

  }

  oid_t GetTaskId() {
    return task_id;
  }

  ResultType GetOuput() {
    return output;
  }

  void *GetArgs() {
    return args;
  }

  handler GetTask() {
    return function_pointer;
  }

  TaskPriorityType GetPriority() {
    return priority;
  }

 protected:
  oid_t task_id;

  handler function_pointer;
  void *args;
  ResultType output;

  TaskPriorityType priority = TaskPriorityType::TASK_PRIORTY_TYPE_NORMAL;
};


} // namespace scheduler
} // namespace peloton
