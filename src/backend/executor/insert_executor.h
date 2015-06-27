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

class InsertExecutor : public AbstractExecutor {

 public:
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor& operator=(const InsertExecutor &) = delete;
  InsertExecutor(InsertExecutor &&) = delete;
  InsertExecutor& operator=(InsertExecutor &&) = delete;

  explicit InsertExecutor(planner::AbstractPlanNode *node,
                          concurrency::Transaction *transaction);

 protected:
  bool DInit();

  bool DExecute();

 private:

};

} // namespace executor
} // namespace nstore
