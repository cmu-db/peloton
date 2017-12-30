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

#include "common/statement.h"
#include "executor/abstract_executor.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace executor {

//===----------------------------------------------------------------------===//
// Plan Executor
//===----------------------------------------------------------------------===//

struct ExecutionResult {
  ResultType m_result;

  // number of tuples processed
  uint32_t m_processed;

  ExecutionResult() {
    m_processed = 0;
    m_result = ResultType::SUCCESS;
  }
};

class PlanExecutor {
 public:
  PlanExecutor() = default;
  DISALLOW_COPY_AND_MOVE(PlanExecutor);

  /*
   * @brief Use std::vector<type::Value> as params to make it more elegant
   * for network
   * Before ExecutePlan, a node first receives value list, so we should
   * pass value list directly rather than passing Postgres's ParamListInfo
   */
  static void ExecutePlan(
      std::shared_ptr<planner::AbstractPlan> plan,
      concurrency::TransactionContext *txn,
      const std::vector<type::Value> &params,
      const std::vector<int> &result_format,
      std::function<void(executor::ExecutionResult,
                         std::vector<ResultValue> &&)> on_complete);

  /*
   * @brief When a peloton node recvs a query plan, this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  // FIXME This should be removed when PelotonService is removed/rewritten
  static int ExecutePlan(
      const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list);
};

}  // namespace executor
}  // namespace peloton
