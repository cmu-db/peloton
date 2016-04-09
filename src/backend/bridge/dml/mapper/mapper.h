//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper.h
//
// Identification: src/backend/bridge/dml/mapper/mapper.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/dml/expr/expr_transformer.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/bridge/dml/mapper/dml_raw_structures.h"
#include "backend/common/logger.h"
#include "backend/common/cache.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/project_info.h"

#include "postgres.h"
#include "c.h"
#include "executor/execdesc.h"
#include "utils/rel.h"

#define PLAN_CACHE_SIZE 100

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

  PlanTransformer() : plan_cache_(PLAN_CACHE_SIZE) {}

  /* Plan caching */
  static PlanTransformer &GetInstance();

  std::shared_ptr<const planner::AbstractPlan> GetCachedPlan(
      const char *prepStmtName);

  /* Plan Mapping */
  static std::unique_ptr<planner::AbstractPlan> TransformPlan(
      AbstractPlanState *planstate);

  std::shared_ptr<const planner::AbstractPlan> TransformPlan(
      AbstractPlanState *planstate, const char *prepStmtName);

  // Analyze the plan
  static void AnalyzePlan(planner::AbstractPlan *plan, PlanState *planstate);

  static std::vector<Value> BuildParams(const ParamListInfo param_list);

 private:
  Cache<std::string, const planner::AbstractPlan> plan_cache_;

  //===--------------------------------------------------------------------===//
  // Options controlling some transform operations
  //===--------------------------------------------------------------------===//

  class TransformOptions {
   public:
    bool use_projInfo = true;  // Use Plan.projInfo or not
    TransformOptions() = default;
    TransformOptions(bool pi) : use_projInfo(pi) {}
  };

  static const TransformOptions DefaultOptions;

  static std::unique_ptr<planner::AbstractPlan> TransformPlan(
      AbstractPlanState *planstate, const TransformOptions options);

  //===--------------------------------------------------------------------===//
  // MODIFY TABLE FAMILY
  //===--------------------------------------------------------------------===//

  static std::unique_ptr<planner::AbstractPlan> TransformModifyTable(
      const ModifyTablePlanState *planstate, const TransformOptions options);

  static std::unique_ptr<planner::AbstractPlan> TransformInsert(
      const ModifyTablePlanState *planstate, const TransformOptions options);
  static std::unique_ptr<planner::AbstractPlan> TransformUpdate(
      const ModifyTablePlanState *planstate, const TransformOptions options);
  static std::unique_ptr<planner::AbstractPlan> TransformDelete(
      const ModifyTablePlanState *planstate, const TransformOptions options);

  //===--------------------------------------------------------------------===//
  // SCAN FAMILY
  //===--------------------------------------------------------------------===//

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
  static std::unique_ptr<planner::AbstractPlan> TransformSeqScan(
      const SeqScanPlanState *planstate, const TransformOptions options);
  static std::unique_ptr<planner::AbstractPlan> TransformIndexScan(
      const IndexScanPlanState *planstate, const TransformOptions options);
  static std::unique_ptr<planner::AbstractPlan> TransformIndexOnlyScan(
      const IndexOnlyScanPlanState *planstate, const TransformOptions options);
  static std::unique_ptr<planner::AbstractPlan> TransformBitmapHeapScan(
      const BitmapHeapScanPlanState *planstate, const TransformOptions options);

  //===--------------------------------------------------------------------===//
  // JOIN FAMILY
  //===--------------------------------------------------------------------===//

  static std::unique_ptr<planner::AbstractPlan> TransformNestLoop(
      const NestLoopPlanState *planstate);

  static std::unique_ptr<planner::AbstractPlan> TransformMergeJoin(
      const MergeJoinPlanState *plan_state);

  static std::unique_ptr<planner::AbstractPlan> TransformHashJoin(
      const HashJoinPlanState *plan_state);

  //===--------------------------------------------------------------------===//
  // OTHERS
  //===--------------------------------------------------------------------===//

  static std::unique_ptr<planner::AbstractPlan> TransformLockRows(
      const LockRowsPlanState *planstate);

  static std::unique_ptr<planner::AbstractPlan> TransformUnique(
      const UniquePlanState *planstate);

  static std::unique_ptr<planner::AbstractPlan> TransformMaterialization(
      const MaterialPlanState *planstate);

  static std::unique_ptr<planner::AbstractPlan> TransformLimit(
      const LimitPlanState *planstate);

  static std::unique_ptr<planner::AbstractPlan> TransformAgg(
      const AggPlanState *plan_state);

  static std::unique_ptr<planner::AbstractPlan> TransformSort(
      const SortPlanState *plan_state);

  static std::unique_ptr<planner::AbstractPlan> TransformHash(
      const HashPlanState *plan_state);

  static PelotonJoinType TransformJoinType(const JoinType type);

  //===--------------------------------------------------------------------===//
  // Common utility functions for Scans
  //===--------------------------------------------------------------------===//

  // Analyze the columns in the plan
  static void GetColumnsAccessed(const planner::AbstractPlan *plan,
                                 std::vector<oid_t> &target_list,
                                 std::vector<oid_t> &qual, oid_t &database_oid,
                                 oid_t &table_id);

  static void GetGenericInfoFromScanState(
      planner::AbstractPlan *&parent,
      expression::AbstractExpression *&predicate,
      std::vector<oid_t> &out_col_list, const AbstractScanPlanState *sstate,
      bool use_projInfo = true);

  static const planner::ProjectInfo *BuildProjectInfo(
      const PelotonProjectionInfo *pi);

  static const planner::ProjectInfo::TargetList BuildTargetList(
      const List *targetList, int column_count);

  static expression::AbstractExpression *BuildPredicateFromQual(List *qual);

  static const std::vector<oid_t> BuildColumnListFromDirectMap(
      planner::ProjectInfo::DirectMapList dmlist);

  static const std::vector<oid_t> BuildColumnListFromTargetList(
      planner::ProjectInfo::TargetList target_list);

  static void BuildColumnListFromExpr(
      std::vector<oid_t> &col_ids,
      const expression::AbstractExpression *expression);

  static const std::vector<oid_t> BuildColumnListFromExpStateList(
      List *expr_state_list);

  static const planner::ProjectInfo *BuildProjectInfoFromTLSkipJunk(
      List *targetLis);
};

}  // namespace bridge
}  // namespace peloton
