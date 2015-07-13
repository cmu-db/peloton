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
  TBBTask(const TBBTask &) = delete;
  TBBTask& operator=(const TBBTask &) = delete;
  TBBTask(TBBTask &&) = delete;
  TBBTask& operator=(TBBTask &&) = delete;

  TBBTask(handler function_pointer, void *args)
 : AbstractTask(function_pointer, args){

  }

  tbb::task* execute() {

    output = (*function_pointer)(args);

    return nullptr;
  }

};


} // namespace scheduler
} // namespace peloton
