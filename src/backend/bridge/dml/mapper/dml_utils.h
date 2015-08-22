//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.h
//
// Identification: src/backend/bridge/ddl/ddl_utils.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/bridge/dml/mapper/dml_raw_structures.h"

#include "postgres.h"
#include "c.h"
#include "nodes/execnodes.h"

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
                                             PlanState *planstate,
                                             bool left_child);

  static AbstractPlanState *peloton_prepare_data(PlanState *planstate);

 private:

  static ModifyTablePlanState *PrepareModifyTableState(
      ModifyTableState *mt_planstate);

  static void PrepareInsertState(ModifyTablePlanState *info,
                                 ModifyTableState *mt_state);

  static void PrepareUpdateState(ModifyTablePlanState *info,
                                 ModifyTableState *mt_state);

  static void PrepareDeleteState(ModifyTablePlanState *info,
                                 ModifyTableState *mt_state);

  static ResultPlanState *PrepareResultState(ResultState *result_state);

  static void PrepareAbstractScanState(AbstractScanPlanState* ss_plan_state,
                                       const ScanState& ss_state);

  static SeqScanPlanState *PrepareSeqScanState(SeqScanState *ss_state);

  static IndexScanPlanState *PrepareIndexScanState(IndexScanState *iss_state);

  static IndexOnlyScanPlanState *PrepareIndexOnlyScanState(
      IndexOnlyScanState *ioss_state);

  static BitmapHeapScanPlanState *PrepareBitmapHeapScanState(
      BitmapHeapScanState *bhss_state);

  static BitmapIndexScanPlanState *PrepareBitmapIndexScanState(
      BitmapIndexScanState *biss_state);

  static LockRowsPlanState *PrepareLockRowsState(LockRowsState *lr_state);

  static LimitPlanState *PrepareLimitState(LimitState *limit_state);

  static MaterialPlanState *PrepareMaterialState(MaterialState *material_state);

  static void PrepareAbstractJoinPlanState(AbstractJoinPlanState* j_plan_state,
                                           const JoinState& j_state);

  static NestLoopPlanState *PrepareNestLoopState(NestLoopState *nl_state);

  static MergeJoinPlanState *PrepareMergeJoinState(MergeJoinState *mj_state);

  static AggPlanState *PrepareAggState(AggState *agg_state);

  static SortPlanState *PrepareSortState(SortState *sort_state);

  static PelotonProjectionInfo *BuildProjectInfo(ProjectionInfo *proj_info,
                                                 int column_count);

};

}  // namespace bridge
}  // namespace peloton
