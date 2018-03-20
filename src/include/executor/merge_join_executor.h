//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// merge_join_executor.h
//
// Identification: src/include/executor/merge_join_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_join_executor.h"
#include "planner/merge_join_plan.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class MergeJoinExecutor : public AbstractJoinExecutor {
  MergeJoinExecutor(const MergeJoinExecutor &) = delete;
  MergeJoinExecutor &operator=(const MergeJoinExecutor &) = delete;

 public:
  explicit MergeJoinExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  size_t Advance(LogicalTile *tile, size_t start_row, bool is_left);

  /** @brief a vector of join clauses
   * Get this from plan node during initialization */
  const std::vector<planner::MergeJoinPlan::JoinClause> *join_clauses_;

  size_t left_start_row = 0;
  size_t right_start_row = 0;

  size_t left_end_row = 0;
  size_t right_end_row = 0;
};

}  // namespace executor
}  // namespace peloton
