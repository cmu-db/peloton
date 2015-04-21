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

namespace nstore {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Abstract Task
//===--------------------------------------------------------------------===//

class AbstractTask : public tbb::task {

 public:
  AbstractTask(catalog::Manager *catalog){
    task_id = catalog->GetNextOid();
  }

  // override this function in all tasks
  tbb::task* execute() {
    return nullptr;
  }

  oid_t GetTaskId() {
    return task_id;
  }

 private:
  oid_t task_id;

};


} // namespace scheduler
} // namespace nstore
