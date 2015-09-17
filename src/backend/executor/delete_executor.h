//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// delete_executor.h
//
// Identification: src/backend/executor/delete_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"

#include <vector>

namespace peloton {
namespace executor {

class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(const DeleteExecutor &) = delete;
  DeleteExecutor &operator=(const DeleteExecutor &) = delete;
  DeleteExecutor(DeleteExecutor &&) = delete;
  DeleteExecutor &operator=(DeleteExecutor &&) = delete;

  DeleteExecutor(planner::AbstractPlan *node,
                 ExecutorContext *executor_context);

  ~DeleteExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  storage::DataTable *target_table_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
