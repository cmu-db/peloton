//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// update_executor.h
//
// Identification: src/backend/executor/update_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/executor/abstract_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/update_plan.h"

namespace peloton {
namespace executor {

class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor &operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  storage::DataTable *target_table_ = nullptr;
  const planner::ProjectInfo *project_info_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
