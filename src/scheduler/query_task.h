/*-------------------------------------------------------------------------
 *
 * query_task.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/query_task.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "parser/parser.h"

namespace nstore {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Query Task
//===--------------------------------------------------------------------===//

class QueryTask : public AbstractTask {

 public:

  // override this function in all tasks
  tbb::task* execute() {

    return nullptr;
  }

};


} // namespace scheduler
} // namespace nstore
