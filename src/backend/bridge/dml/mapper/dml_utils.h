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
                                 ModifyTableState *mt_plan_state);

  static void PrepareUpdateState(ModifyTablePlanState *info,
                                 ModifyTableState *mt_plan_state);

  static void PrepareDeleteState(ModifyTablePlanState *info,
                                 ModifyTableState *mt_plan_state);

  static ResultPlanState *PrepareResultState(ResultState *result_plan_state);

  static void PrepareAbstractScanState(AbstractScanPlanState* ss_plan_state,
                                       const ScanState& ss_state);

  static SeqScanPlanState *PrepareSeqScanState(SeqScanState *ss_plan_state);

  static IndexScanPlanState *PrepareIndexScanState(
      IndexScanState *iss_plan_state);

  static IndexOnlyScanPlanState *PrepareIndexOnlyScanState(
      IndexOnlyScanState *ioss_plan_state);

  static BitmapHeapScanPlanState *PrepareBitmapHeapScanState(
      BitmapHeapScanState *bhss_plan_state);

  static LockRowsPlanState *PrepareLockRowsState(LockRowsState *lr_plans_tate);

  static LimitPlanState *PrepareLimitState(LimitState *limit_plan_state);

  static MaterialPlanState *PrepareMaterialState(
      MaterialState *material_plan_state);

  static NestLoopPlanState *PrepareNestLoopState(NestLoopState *nl_plan_state);

  static AggPlanState *PrepareAggPlanState(AggState *agg_plan_state);

  static PelotonProjectionInfo *BuildProjectInfo(ProjectionInfo *proj_info,
                                                 int column_count);

};

}  // namespace bridge
}  // namespace peloton
