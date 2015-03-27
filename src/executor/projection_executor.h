/**
 * @brief Header file for projection executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"

namespace nstore {
namespace executor {

class ProjectionExecutor : public AbstractExecutor {
 public:
  ProjectionExecutor(planner::AbstractPlanNode *abstract_node) :
    AbstractExecutor(abstract_node) {
  }

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();

  void SubCleanUp();
};

} // namespace executor
} // namespace nstore
