//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.h
//
// Identification: src/include/executor/delete_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(const DeleteExecutor &) = delete;
  DeleteExecutor &operator=(const DeleteExecutor &) = delete;
  DeleteExecutor(DeleteExecutor &&) = delete;
  DeleteExecutor &operator=(DeleteExecutor &&) = delete;

  DeleteExecutor(const planner::AbstractPlan *node,
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
