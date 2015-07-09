/**
 * @brief Header for postgres plan transformer.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/execdesc.h"
#include "backend/planner/abstract_plan_node.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Transformer (From Postgres To Peloton)
//===--------------------------------------------------------------------===//

class PlanTransformer {

 public:
  PlanTransformer(const PlanTransformer &) = delete;
  PlanTransformer& operator=(const PlanTransformer &) = delete;
  PlanTransformer(PlanTransformer &&) = delete;
  PlanTransformer& operator=(PlanTransformer &&) = delete;

  PlanTransformer(){};

  static void PrintPlanState(const PlanState *plan_state);

  static planner::AbstractPlanNode *TransformPlan(const PlanState *plan_state);

  /* TODO: Is this a good place to have the function? */
  static bool CleanPlanNodeTree(planner::AbstractPlanNode *root);

 private:

  static planner::AbstractPlanNode *TransformModifyTable(const ModifyTableState *plan_state);

  static planner::AbstractPlanNode *TransformInsert(const ModifyTableState *plan_state);
  static planner::AbstractPlanNode *TransformUpdate(const ModifyTableState *plan_state);
  static planner::AbstractPlanNode *TransformDelete(const ModifyTableState *plan_state);

  static planner::AbstractPlanNode *TransformSeqScan(const SeqScanState *plan_state);

  static planner::AbstractPlanNode *TransformResult(const ResultState *plan_state);

};

} // namespace bridge
} // namespace peloton
