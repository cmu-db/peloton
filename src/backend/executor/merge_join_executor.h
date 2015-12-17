/*-------------------------------------------------------------------------
 *
 * merge_join.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/merge_join_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "backend/executor/abstract_join_executor.h"
#include "backend/planner/merge_join_plan.h"

namespace peloton {
namespace executor {

class MergeJoinExecutor : public AbstractJoinExecutor {
  MergeJoinExecutor(const MergeJoinExecutor &) = delete;
  MergeJoinExecutor &operator=(const MergeJoinExecutor &) = delete;

 public:
  explicit MergeJoinExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

  bool left_end_ = false;

  bool right_end_ = false;

 private:

  size_t Advance(LogicalTile *tile, size_t start_row, bool is_left);

  std::vector<std::unique_ptr<LogicalTile> > left_tiles_;
  std::vector<std::unique_ptr<LogicalTile> > right_tiles_;

  /** @brief a vector of join clauses
   * Get this from plan node while init */
  const std::vector<planner::MergeJoinPlan::JoinClause> *join_clauses_;

};

}  // namespace executor
}  // namespace peloton
