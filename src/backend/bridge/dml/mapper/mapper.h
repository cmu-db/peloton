//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper.h
//
// Identification: src/backend/bridge/dml/mapper/mapper.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../../planner/abstract_plan.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/dml/expr/expr_transformer.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/common/value_vector.h"
#include "backend/common/logger.h"
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

  static void PrintPlanState(const PlanState *plan);

  static planner::AbstractPlan *TransformPlan(const PlanState *plan) {
    return TransformPlan(plan, kDefaultOptions);
  }

  static bool CleanPlanNodeTree(planner::AbstractPlan *root);

  static const ValueArray BuildParams(const ParamListInfo param_list);

 private:
  //======-----------------------------------
  // Options controlling some transform operations
  //======-----------------------------------
  class TransformOptions {
   public:
    bool use_projInfo = true;  // Use PlanState.projInfo or not
    TransformOptions() = default;
    TransformOptions(bool pi) : use_projInfo(pi) {}
  };

  static const TransformOptions kDefaultOptions;

  static planner::AbstractPlan *TransformPlan(
      const PlanState *plan, const TransformOptions options);

  //======---------------------------------------
  // MODIFY TABLE FAMILY
  //======---------------------------------------
  static planner::AbstractPlan *TransformModifyTable(
      const ModifyTableState *plan_state, const TransformOptions options);

  static planner::AbstractPlan *TransformInsert(
      const ModifyTableState *plan_state, const TransformOptions options);
  static planner::AbstractPlan *TransformUpdate(
      const ModifyTableState *plan_state, const TransformOptions options);
  static planner::AbstractPlan *TransformDelete(
      const ModifyTableState *plan_state, const TransformOptions options);

  //======---------------------------------------
  // SCAN FAMILY
  //======---------------------------------------
  /*
   * The ScanState.projInfo in ScanState may be processed in three possible
   * ways:
   * 1. It is stolen by the scan's parent. Then, options.use_projInfo should be
   * false
   * and the transform methods will skip processing projInfo.
   * 2. It is a pure direct map, which will be converted to a column list for
   * AbstractScanNode.
   * 3. It contains non-trivial projections. In this case, the transform methods
   * will
   * generate a projection plan node and put it on top of the scan node.
   */
  static planner::AbstractPlan *TransformSeqScan(
      const SeqScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlan *TransformIndexScan(
      const IndexScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlan *TransformIndexOnlyScan(
      const IndexOnlyScanState *plan_state, const TransformOptions options);
  static planner::AbstractPlan *TransformBitmapScan(
      const BitmapHeapScanState *plan_state, const TransformOptions options);

  //======---------------------------------------
  // JOIN FAMILY
  //======---------------------------------------
  static planner::AbstractPlan *TransformNestLoop(
      const NestLoopState *plan_state);

  //======---------------------------------------
  // OTHERS
  //======---------------------------------------
  static planner::AbstractPlan *TransformLockRows(
      const LockRowsState *plan_state);

  static planner::AbstractPlan *TransformMaterialization(
      const MaterialState *plan_state);

  static planner::AbstractPlan *TransformLimit(
      const LimitState *plan_state);
  static planner::AbstractPlan *TransformResult(
      const ResultState *plan_state);

  static PelotonJoinType TransformJoinType(const JoinType type);

  //========-----------------------------------------
  // Common utility functions for Scan's
  //========-----------------------------------------
  static void GetGenericInfoFromScanState(
      planner::AbstractPlan *&parent,
      expression::AbstractExpression *&predicate,
      std::vector<oid_t> &out_col_list, const ScanState *sstate,
      bool use_projInfo = true);

  static const planner::ProjectInfo *BuildProjectInfo(
      const ProjectionInfo *pg_proj_info, oid_t column_count);

  static expression::AbstractExpression *BuildPredicateFromQual(List *qual);

  static const std::vector<oid_t> BuildColumnListFromDirectMap(
      planner::ProjectInfo::DirectMapList dmlist);
};

}  // namespace bridge
}  // namespace peloton
