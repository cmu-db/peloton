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

#include "backend/planner/abstract_plan.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/dml/expr/expr_transformer.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/common/value_vector.h"
#include "backend/common/logger.h"
#include "backend/planner/project_info.h"
#include "backend/bridge/dml/mapper/dml_raw_structures.h"

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

  static void PrintPlan(const Plan *plan);

  static planner::AbstractPlan *TransformPlan(planner::AbstractPlanState *planstate) {
    return TransformPlan(planstate, kDefaultOptions);
  }

  static bool CleanPlanTree(planner::AbstractPlan *root);

  static bool CleanPlanStateTree(planner::AbstractPlanState *root);

  static const ValueArray BuildParams(const ParamListInfo param_list);

 private:
  //======-----------------------------------
  // Options controlling some transform operations
  //======-----------------------------------
  class TransformOptions {
   public:
    bool use_projInfo = true;  // Use Plan.projInfo or not
    TransformOptions() = default;
    TransformOptions(bool pi) : use_projInfo(pi) {}
  };

  static const TransformOptions kDefaultOptions;

  static planner::AbstractPlan *TransformPlan(
      planner::AbstractPlanState *planstate,
      const TransformOptions options);

  //======---------------------------------------
  // MODIFY TABLE FAMILY
  //======---------------------------------------
  static planner::AbstractPlan *TransformModifyTable(
      planner::ModifyTablePlanState *planstate,
      const TransformOptions options);

  static planner::AbstractPlan *TransformInsert(
      planner::ModifyTablePlanState *planstate,
      const TransformOptions options);
  static planner::AbstractPlan *TransformUpdate(
      planner::ModifyTablePlanState *planstate,
      const TransformOptions options);
  static planner::AbstractPlan *TransformDelete(
      planner::ModifyTablePlanState *planstate,
      const TransformOptions options);

  //======---------------------------------------
  // SCAN FAMILY
  //======---------------------------------------
  /*
   * The Scan.projInfo in Scan may be processed in three possible
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
      planner::SeqScanPlanState *planstate,
      const TransformOptions options);
  static planner::AbstractPlan *TransformIndexScan(
      planner::IndexScanPlanState *planstate,
      const TransformOptions options);
  static planner::AbstractPlan *TransformIndexOnlyScan(
      planner::IndexOnlyScanPlanState *planstate,
      const TransformOptions options);
  static planner::AbstractPlan *TransformBitmapScan(
      planner::BitmapHeapScanPlanState *planstate,
      const TransformOptions options);

  //======---------------------------------------
  // JOIN FAMILY
  //======---------------------------------------
  static planner::AbstractPlan *TransformNestLoop(
      planner::NestLoopPlanState *planstate);

  //======---------------------------------------
  // OTHERS
  //======---------------------------------------
  static planner::AbstractPlan *TransformLockRows(
      planner::LockRowsPlanState *planstate);

  static planner::AbstractPlan *TransformMaterialization(
      planner::MaterialPlanState *planstate);

  static planner::AbstractPlan *TransformLimit(
      planner::LimitPlanState *planstate);

  static planner::AbstractPlan *TransformResult(
      planner::ResultPlanState *planstate);

  static PelotonJoinType TransformJoinType(const JoinType type);

  //========-----------------------------------------
  // Common utility functions for Scan's
  //========-----------------------------------------

  static void GetGenericInfoFromScan(
      planner::AbstractPlan *parent,
      expression::AbstractExpression *predicate,
      std::vector<oid_t> &out_col_list,
      planner::AbstractScanPlanState *sstate,
      bool use_projInfo = true);

  static const planner::ProjectInfo *BuildProjectInfo(
      const ProjectionInfo *pg_proj_info, oid_t column_count);

  static expression::AbstractExpression *BuildPredicateFromQual(List *qual);

  static const std::vector<oid_t> BuildColumnListFromDirectMap(
      planner::ProjectInfo::DirectMapList dmlist);
};

}  // namespace bridge
}  // namespace peloton
