//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/include/executor/plan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/statement.h"
#include "executor/abstract_executor.h"
#include "type/types.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace bridge {

//===----------------------------------------------------------------------===//
// Plan Executor
//===----------------------------------------------------------------------===//

typedef struct peloton_status {
  peloton::ResultType m_result;
  int *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  peloton_status() {
    m_processed = 0;
    m_result = peloton::ResultType::SUCCESS;
    m_result_slots = nullptr;
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(peloton::SerializeOutput &output);
  bool DeserializeFrom(peloton::SerializeInput &input);

} peloton_status;

class PlanExecutor {
 public:
  PlanExecutor(){};

  // Copy From
  static inline void copyFromTo(const std::string &src,
                                std::vector<unsigned char> &dst) {
    if (src.c_str() == nullptr) {
      return;
    }
    size_t len = src.size();
    for (unsigned int i = 0; i < len; i++) {
      dst.push_back((unsigned char)src[i]);
    }
  }

  /*
   * @brief Use std::vector<type::Value> as params to make it more elegant
   * for networking
   * Before ExecutePlan, a node first receives value list, so we should
   * pass value list directly rather than passing Postgres's ParamListInfo
   */
  static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                    concurrency::Transaction* txn,
                                    const std::vector<type::Value> &params,
                                    std::vector<StatementResult> &result,
                                    const std::vector<int> &result_format);

  /*
   * @brief When a peloton node recvs a query plan, this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  static int ExecutePlan(
      const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list);

 private:
  DISALLOW_COPY_AND_MOVE(PlanExecutor);
};

}  // namespace bridge
}  // namespace peloton
