//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/bridge/dml/executor/plan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "executor/abstract_executor.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Executor
//===--------------------------------------------------------------------===//

typedef struct peloton_status {
  peloton::Result m_result;
  int *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  peloton_status(){
    m_processed = 0;
    m_result = peloton::RESULT_SUCCESS;
    m_result_slots = nullptr;
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(peloton::SerializeOutput &output);
  bool DeserializeFrom(peloton::SerializeInputBE &input);

} peloton_status;

class PlanExecutor {
 public:
  PlanExecutor(const PlanExecutor &) = delete;
  PlanExecutor &operator=(const PlanExecutor &) = delete;
  PlanExecutor(PlanExecutor &&) = delete;
  PlanExecutor &operator=(PlanExecutor &&) = delete;

  PlanExecutor(){};

  static void PrintPlan(const planner::AbstractPlan *plan,
                        std::string prefix = "");

/* TODO: Delete this mothod
  static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                    ParamListInfo m_param_list,
                                    TupleDesc m_tuple_desc);
*/

  /*
   * @brief Use std::vector<Value> as params to make it more elegant for networking
   *        Before ExecutePlan, a node first receives value list, so we should pass
   *        value list directly rather than passing Postgres's ParamListInfo
   */
  static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                    const std::vector<Value> &params);

  /*
   * @brief When a peloton node recvs a query plan, this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  static int ExecutePlan(const planner::AbstractPlan *plan,
                         const std::vector<Value> &params,
                         std::vector<std::unique_ptr<executor::LogicalTile>>&
                         logical_tile_list);

};

}  // namespace bridge
}  // namespace peloton
