/*-------------------------------------------------------------------------
 *
 * tbb_task.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/scheduler/tbb_task.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/scheduler/abstract_task.h"
#include "tbb/tbb.h"

#include <iostream>

namespace peloton {
namespace scheduler {

//===--------------------------------------------------------------------===//
// TBB Task
//===--------------------------------------------------------------------===//

class TBBTask : public AbstractTask, public tbb::task {

 public:

  TBBTask(handler function_pointer, void *args)
 : AbstractTask(function_pointer, args){

  }

  tbb::task* execute() {

    std::cout << "Starting task \n";
    output = (*function_pointer)(args);
    std::cout << "Stopping task \n";

    return nullptr;
  }

};


} // namespace scheduler
} // namespace peloton
