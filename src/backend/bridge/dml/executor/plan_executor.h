//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// plan_executor.h
//
// Identification: src/backend/bridge/dml/executor/plan_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

  static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                    ParamListInfo m_param_list,
                                    TupleDesc m_tuple_desc,
                                    TransactionId txn_id);

  static executor::AbstractExecutor *AddMaterialization(
      executor::AbstractExecutor *root);

 private:
};

}  // namespace bridge
}  // namespace peloton
