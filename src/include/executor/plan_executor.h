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
#include "type/types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace executor {

//===----------------------------------------------------------------------===//
// Plan Executor
//===----------------------------------------------------------------------===//

typedef struct ExecuteResult {
  peloton::ResultType m_result;
  int *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  ExecuteResult() {
    m_processed = 0;
    m_result = peloton::ResultType::SUCCESS;
    m_result_slots = nullptr;
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(peloton::SerializeOutput &output);
  bool DeserializeFrom(peloton::SerializeInput &input);

} ExecuteResult;

class PlanExecutor {
 public:
  PlanExecutor(){};

  /*
   * @brief Use std::vector<type::Value> as params to make it more elegant
   * for network
   * Before ExecutePlan, a node first receives value list, so we should
   * pass value list directly rather than passing Postgres's ParamListInfo
   */
  static void ExecutePlan(std::shared_ptr<planner::AbstractPlan> plan,
                                   concurrency::TransactionContext* txn,
                                   const std::vector<type::Value> &params,
                                   std::vector<ResultValue> &result,
                                   const std::vector<int> &result_format,
                                   executor::ExecuteResult &p_status);

  /*
   * @brief When a peloton node recvs a query plan, this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  // FIXME This should be removed when PelotonService is removed/rewritten
  static int ExecutePlan(
      const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list);

 private:
  DISALLOW_COPY_AND_MOVE(PlanExecutor);
};

}  // namespace executor
}  // namespace peloton
