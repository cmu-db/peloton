/**
 * @brief Header for postgres plan transformer.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/dml/expr/expr_transformer.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/common/value_vector.h"
#include "backend/common/logger.h"
#include "backend/planner/abstract_plan_node.h"
#include "backend/planner/project_info.h"

#include "postgres.h"
#include "c.h"
#include "executor/execdesc.h"
#include "utils/rel.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Transformer (From Postgres To Peloton)
//===--------------------------------------------------------------------===//

class PlanTransformer {
 public:
  PlanTransformer(const PlanTransformer &) = delete;
  PlanTransformer &operator=(const PlanTransformer &) = delete;
  PlanTransformer(PlanTransformer &&) = delete;
  PlanTransformer &operator=(PlanTransformer &&) = delete;

  PlanTransformer(){};

  static void PrintPlanState(const PlanState *plan_state);

  static planner::AbstractPlanNode *TransformPlan(const PlanState *plan_state){
    return TransformPlan(plan_state, kDefaultOptions);
  }

  /* TODO: Is this a good place to have the function? */
  static bool CleanPlanNodeTree(planner::AbstractPlanNode *root);

  static const ValueArray BuildParams(const ParamListInfo param_list);

 private:
  //======-----------------------------------
  // Options controlling some transform operations
  //======-----------------------------------
  class TransformOptions {
   public:
    bool use_projInfo = true; // Use PlanState.projInfo or not
    TransformOptions() = default;
    TransformOptions(bool pi)
      :use_projInfo(pi){}
  };

  static const TransformOptions kDefaultOptions;

  static planner::AbstractPlanNode *TransformPlan(
      const PlanState *plan_state, const TransformOptions options );


  static planner::AbstractPlanNode *TransformModifyTable(
      const ModifyTableState *plan_state, const TransformOptions options);

  static planner::AbstractPlanNode *TransformInsert(
      const ModifyTableState *plan_state, const TransformOptions options);
  static planner::AbstractPlanNode *TransformUpdate(
      const ModifyTableState *plan_state, const TransformOptions options);
  static planner::AbstractPlanNode *TransformDelete(
      const ModifyTableState *plan_state, const TransformOptions options);

  static planner::AbstractPlanNode *TransformSeqScan(
      const SeqScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlanNode *TransformIndexScan(
      const IndexScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlanNode *TransformIndexOnlyScan(
      const IndexOnlyScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlanNode *TransformBitmapScan(
      const BitmapHeapScanState *plan_state, const TransformOptions options);

  static planner::AbstractPlanNode *TransformNestLoop(
      const NestLoopState *plan_state);

  static planner::AbstractPlanNode *TransformLockRows(
      const LockRowsState *plan_state);

  static planner::AbstractPlanNode *TransformLimit(
      const LimitState *plan_state);
  static planner::AbstractPlanNode *TransformResult(
      const ResultState *plan_state);



  static PelotonJoinType TransformJoinType(const JoinType type);


  /*
   * ======================================================================
   * Common utility functions for Scan's
   * ======================================================================
   */

  static void GetGenericInfoFromScanState(planner::AbstractPlanNode*& parent,
                                   expression::AbstractExpression*& predicate,
                                   std::vector<oid_t>& out_col_list,
                                   const ScanState* sstate,
                                   bool use_projInfo = true);

  static const planner::ProjectInfo *BuildProjectInfo(
      const ProjectionInfo *pg_proj_info, oid_t column_count);

  static expression::AbstractExpression* BuildPredicateFromQual(List* qual);

  static const std::vector<oid_t> BuildColumnListFromDirectMap(planner::ProjectInfo::DirectMapList dmlist);

};

}  // namespace bridge
}  // namespace peloton
