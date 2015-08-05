//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_task.h
//
// Identification: src/backend/scheduler/abstract_task.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/scheduler/abstract_task.h"

#include "tbb/tbb.h"

namespace peloton {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Task
//===--------------------------------------------------------------------===//

typedef Result (*handler)(void *);

class AbstractTask : public tbb::task {
 public:
  AbstractTask(handler function_pointer, void *args, TaskPriorityType priority)
      : function_pointer(function_pointer),
        args(args),
        output(RESULT_INVALID),
        priority(priority) {
    // Get a task id
    task_id = catalog::Manager::GetInstance().GetNextOid();
  }

  tbb::task *execute() {
    LOG_TRACE("Starting task \n");
    output = (*function_pointer)(args);
    LOG_TRACE("Stopping task \n");

    return nullptr;
  }

  oid_t GetTaskId() { return task_id; }

  Result GetOuput() { return output; }

  void *GetArgs() { return args; }

  handler GetTask() { return function_pointer; }

  TaskPriorityType GetPriority() { return priority; }

 protected:
  oid_t task_id;

  handler function_pointer;

  void *args;

  Result output;

  TaskPriorityType priority = TaskPriorityType::TASK_PRIORTY_TYPE_NORMAL;
};

}  // namespace scheduler
}  // namespace peloton
