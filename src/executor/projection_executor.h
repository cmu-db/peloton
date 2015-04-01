/**
 * @brief Header file for projection executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {

namespace planner {
  class ProjectionNode;
}

namespace executor {
class LogicalTile;

class ProjectionExecutor : public AbstractExecutor {
 public:
  ProjectionExecutor(const planner::ProjectionNode *node);

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();

  void SubCleanUp();

 private:
  /** @brief Projection node corresponding to this executor. */
  const planner::ProjectionNode *node_;
};

} // namespace executor
} // namespace nstore
