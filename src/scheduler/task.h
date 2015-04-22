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
// Task Priority Types
//===--------------------------------------------------------------------===//

enum TaskPriorityType {
    TASK_PRIORTY_TYPE_INVALID          = 0, // invalid priority

    TASK_PRIORTY_TYPE_LOW              = 10,
    TASK_PRIORTY_TYPE_NORMAL           = 11,
    TASK_PRIORTY_TYPE_HIGH             = 12
};

//===--------------------------------------------------------------------===//
// Task
//===--------------------------------------------------------------------===//

class Task : public tbb::task {

 public:
  Task(void *(*function_pointer)(void*), void *args)
 : function_pointer(function_pointer),
   args(args),
   output(nullptr){

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

  void *GetOuput() {
    return output;
  }

 protected:
  oid_t task_id;

  void *(*function_pointer)(void*);
  void *args;
  void *output;
};


} // namespace scheduler
} // namespace nstore
