/**
 * @brief Header file for insert executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/expression/abstract_expression.h"

#include <vector>

namespace peloton {
namespace executor {

class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor& operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(planner::AbstractPlanNode *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  storage::DataTable *target_table_ = nullptr;
  std::vector<std::pair<oid_t, expression::AbstractExpression*>> updated_col_exprs_;
};

} // namespace executor
} // namespace peloton
