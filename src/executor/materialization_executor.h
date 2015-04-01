/**
 * @brief Header file for materialization executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {

namespace planner {
  class MaterializationNode;
}

namespace executor {

class MaterializationExecutor : public AbstractExecutor {
 public:
  MaterializationExecutor(const planner::MaterializationNode *node);

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();

  void SubCleanUp();

 private:
  /** @brief Materialization node corresponding to this executor. */
  const planner::MaterializationNode *node_;
};

} // namespace executor
} // namespace nstore
