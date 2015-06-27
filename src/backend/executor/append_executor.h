/**
 * @brief Header file for append executor.
 *
 * Copyright(c) 2015, CMU
 */
#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"


namespace nstore {
namespace executor {

/**
 * @brief Append executor.
 * Trivially concatenate input tiles from the children.
 * No check on the schemas of children.
 */
class AppendExecutor : public AbstractExecutor {
 public:
  AppendExecutor(const AppendExecutor &) = delete;
  AppendExecutor& operator=(const AppendExecutor &) = delete;
  AppendExecutor(const AppendExecutor &&) = delete;
  AppendExecutor& operator=(const AppendExecutor &&) = delete;

  explicit AppendExecutor(planner::AbstractPlanNode *node,
                          concurrency::Transaction *transaction);

 protected:
  bool DInit();
  bool DExecute();

 private:
  size_t cur_child_id_ = 0;
};


}
}
