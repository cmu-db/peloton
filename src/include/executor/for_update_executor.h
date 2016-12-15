//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// for_update_executor.h
//
// Identification: src/include/executor/for_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>

#include "executor/abstract_executor.h"
#include "expression/abstract_expression.h"
#include "planner/update_plan.h"

namespace peloton {
namespace executor {

class ForUpdateExecutor : public AbstractExecutor {
  ForUpdateExecutor(const ForUpdateExecutor &) = delete;
  ForUpdateExecutor &operator=(const ForUpdateExecutor &) = delete;

 public:
  explicit ForUpdateExecutor(const planner::AbstractPlan *node,
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
