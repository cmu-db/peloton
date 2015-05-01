/**
 * @brief Header file for insert executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

class InsertExecutor : public AbstractExecutor {
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor& operator=(const InsertExecutor &) = delete;

 public:
  explicit InsertExecutor(planner::AbstractPlanNode *node);

 protected:
  bool DInit();

  bool DExecute();
};

} // namespace executor
} // namespace nstore
