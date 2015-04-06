/**
 * @brief Header file for materialization executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {

namespace planner {
  class AbstractPlanNode;
}

namespace executor {

class MaterializationExecutor : public AbstractExecutor {
 public:
  explicit MaterializationExecutor(const planner::AbstractPlanNode *node);

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();

  void SubCleanUp();
};

} // namespace executor
} // namespace nstore
