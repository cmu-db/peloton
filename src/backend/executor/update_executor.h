/**
 * @brief Header file for insert executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/executor/abstract_executor.h"

#include <vector>

namespace nstore {
namespace executor {

class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor& operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(planner::AbstractPlanNode *node,
                          concurrency::Transaction *context);

 protected:
  bool DInit();

  bool DExecute();

 private:

};

} // namespace executor
} // namespace nstore
