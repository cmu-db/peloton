/**
 * @brief Header file for projection executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "planner/projection_node.h"

#include <unordered_set>
#include <vector>

namespace nstore {
namespace executor {

class ProjectionExecutor : public AbstractExecutor {
 public:
  ProjectionExecutor(
      std::unique_ptr<planner::AbstractPlanNode> abstract_node,
      std::vector<AbstractExecutor *>& children);

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();

  void SubCleanUp();

 private:
  /**
   * @brief Set of output column ids.
   *  
   * Ids which are not in this set are invalidated by this executor.
   */
  std::unordered_set<id_t> output_column_ids_;
    
};

} // namespace executor
} // namespace nstore
