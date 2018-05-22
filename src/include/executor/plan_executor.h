//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/include/executor/plan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "common/statement.h"
#include "executor/logical_tile.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace type {
class Value;
}  // namespace type

namespace executor {

/**
 * The result of the execution of a query/
 */
struct ExecutionResult {
  ResultType m_result;

  // number of tuples processed
  uint32_t m_processed;

  // string of error message
  std::string m_error_message;

  ExecutionResult() {
    m_processed = 0;
    m_result = ResultType::SUCCESS;
    m_error_message = "";
  }
};

class PlanExecutor {
 public:
  /**
   * This function executes a single query in a transactional context.  The
   * provided callback is called with the results of the execution with it
   * completes.
   *
   * @param plan The physical query plan that will be run
   * @param txn The transactional context the query will run in
   * @param params All parameters the query references
   * @param result_format No idea ...
   * @param on_complete The callback function to invoke when the query finishes.
   */
  static void ExecutePlan(
      std::shared_ptr<planner::AbstractPlan> plan,
      concurrency::TransactionContext *txn,
      const std::vector<type::Value> &params,
      const std::vector<int> &result_format,
      std::function<void(executor::ExecutionResult,
                         std::vector<ResultValue> &&)> on_complete);

  /**
   * @brief When a peloton node recvs a query plan, this function is invoked
   *
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  // FIXME This should be removed when PelotonService is removed/rewritten
  static int ExecutePlan(
      planner::AbstractPlan *plan, const std::vector<type::Value> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list);
};

}  // namespace executor
}  // namespace peloton
