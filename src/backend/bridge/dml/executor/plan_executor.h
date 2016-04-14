//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/backend/bridge/dml/executor/plan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"

#include "postgres.h"
#include "access/tupdesc.h"
#include "postmaster/peloton.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Executor
//===--------------------------------------------------------------------===//

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
   * Use std::vector<Value> as params to make it more elegant for networking
   * Before ExecutePlan, a node first receives value list, so we should pass
   * value list directly rather than passing Postgres's ParamListInfo
   */
  static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                    const std::vector<Value> &params,
                                    TupleDesc m_tuple_desc);

  static int ExecutePlan(const planner::AbstractPlan *plan,
                         const std::vector<Value> &params,
                         std::vector<std::unique_ptr<executor::LogicalTile>>&
                         logical_tile_list);

 private:
};

}  // namespace bridge
}  // namespace peloton
