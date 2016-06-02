//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dml_utils.h
//
// Identification: src/bridge/dml/mapper/dml_utils.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "bridge/dml/mapper/dml_raw_structures.h"

#include "postgres.h"
#include "c.h"
#include "nodes/execnodes.h"
#include "utils/lsyscache.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML UTILS
//===--------------------------------------------------------------------===//

class DMLUtils {
 public:
  DMLUtils(const DMLUtils &) = delete;
  DMLUtils &operator=(const DMLUtils &) = delete;
  DMLUtils(DMLUtils &&) = delete;
  DMLUtils &operator=(DMLUtils &&) = delete;

  static AbstractPlanState *PreparePlanState(AbstractPlanState *root,
                                             const PlanState *planstate,
                                             bool left_child);

  static AbstractPlanState *peloton_prepare_data(const PlanState *planstate);

 private:
  static ModifyTablePlanState *PrepareModifyTableState(
      const ModifyTableState *mt_planstate);

  static void PrepareInsertState(ModifyTablePlanState *info,
                                 const ModifyTableState *mt_state);

  static void PrepareUpdateState(ModifyTablePlanState *info,
                                 const ModifyTableState *mt_state);

  static void PrepareDeleteState(ModifyTablePlanState *info,
                                 const ModifyTableState *mt_state);

  static ResultPlanState *PrepareResultState(const ResultState *result_state);

  static UniquePlanState *PrepareUniqueState(const UniqueState *result_state);

  static void PrepareAbstractScanState(AbstractScanPlanState *ss_plan_state,
                                       const ScanState &ss_state);

  static SeqScanPlanState *PrepareSeqScanState(const SeqScanState *ss_state);

  static IndexScanPlanState *PrepareIndexScanState(
      const IndexScanState *iss_state);

  static IndexOnlyScanPlanState *PrepareIndexOnlyScanState(
      const IndexOnlyScanState *ioss_state);

  static BitmapHeapScanPlanState *PrepareBitmapHeapScanState(
      const BitmapHeapScanState *bhss_state);

  static BitmapIndexScanPlanState *PrepareBitmapIndexScanState(
      const BitmapIndexScanState *biss_state);

  static LockRowsPlanState *PrepareLockRowsState(const LockRowsState *lr_state);

  static LimitPlanState *PrepareLimitState(const LimitState *limit_state);

  static MaterialPlanState *PrepareMaterialState(
      const MaterialState *material_state);

  static void PrepareAbstractJoinPlanState(AbstractJoinPlanState *j_plan_state,
                                           const JoinState &j_state);

  static NestLoopPlanState *PrepareNestLoopState(const NestLoopState *nl_state);

  static MergeJoinPlanState *PrepareMergeJoinState(
      const MergeJoinState *mj_state);

  static HashJoinPlanState *PrepareHashJoinState(const HashJoinState *hj_state);

  static AggPlanState *PrepareAggState(const AggState *agg_state);

  static SortPlanState *PrepareSortState(const SortState *sort_plan_state) {
    SortPlanState *info = (SortPlanState *)(palloc(sizeof(SortPlanState)));
    info->type = sort_plan_state->ss.ps.type;
    info->sort = (const Sort *)(copyObject(sort_plan_state->ss.ps.plan));
    info->reverse_flags = (bool *)(palloc(sizeof(bool) * info->sort->numCols));
    // Find the reverse flags here
    for (int i = 0; i < info->sort->numCols; i++) {
      Oid orderingOp = info->sort->sortOperators[i];
      Oid opfamily;
      Oid opcintype;
      int16 strategy;

      /* Find the operator___ in pg_amop */
      if (!get_ordering_op_properties(orderingOp, &opfamily, &opcintype,
                                      &strategy)) {
        elog(ERROR, "operator %u is not a valid ordering operator", orderingOp);
      }

      bool reverse = (strategy == BTGreaterStrategyNumber);

      info->reverse_flags[i] = reverse;
    }
    return info;
  }

  static HashPlanState *PrepareHashState(const HashState *hash_state);

  static PelotonProjectionInfo *BuildProjectInfo(ProjectionInfo *proj_info,
                                                 int column_count);
};

}  // namespace bridge
}  // namespace peloton
