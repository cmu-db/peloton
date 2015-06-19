/*-------------------------------------------------------------------------
 *
 * nested_loop_join.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/nested_loop_join_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/schema.h"
#include "executor/abstract_executor.h"
#include "planner/nested_loop_join_node.h"

#include <vector>

namespace nstore {
namespace executor {

class NestedLoopJoinExecutor : public AbstractExecutor {
  NestedLoopJoinExecutor(const NestedLoopJoinExecutor &) = delete;
  NestedLoopJoinExecutor& operator=(const NestedLoopJoinExecutor &) = delete;

 public:
  explicit NestedLoopJoinExecutor(planner::AbstractPlanNode *node);

 protected:
  bool DInit();

  bool DExecute();

 private:

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of nested loop join. */
  std::vector<LogicalTile *> result;

  /** @brief Starting left table scan. */
  bool left_scan_start = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Join predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

};

} // namespace executor
} // namespace nstore
