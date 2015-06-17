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

  /** @brief Result itr */
  size_t shorter_table_itr = 0;

  /** @brief Computed the result */
  bool done = false;

  /** @brief Result itr */
  size_t result_itr = 0;

  /** @brief Join table schema. */
  const catalog::Schema *schema_ = nullptr;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Join predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

};

} // namespace executor
} // namespace nstore
