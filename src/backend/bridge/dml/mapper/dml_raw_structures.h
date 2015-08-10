/*-------------------------------------------------------------------------
 *
 * dml_raw_structures.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/dml_raw_structures.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/planner/abstract_plan.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// DDL raw data structures
//===--------------------------------------------------------------------===//

struct ModifyTablePlanState : public AbstractPlanState {

  CmdType operation;
  Oid table_oid;

  AbstractPlanState **mt_plans;   /* subplans (one per target rel) */
  int     mt_nplans;    /* number of plans in the array */
  int     mt_whichplan; /* which one is being executed (0..n-1) */

};

struct LockRowsPlanState : public AbstractPlanState {

};

struct AbstractScanPlanState : public AbstractPlanState {

  Oid table_oid;

  ProjectionInfo *ps_ProjInfo;
  TupleDesc tts_tupleDescriptor;

  List *qual;

};

struct SeqScanPlanState : public AbstractScanPlanState {

};

struct IndexScanPlanState : public AbstractScanPlanState {

  int iss_NumScanKeys;
  ScanKey   iss_ScanKeys;

};

struct BitmapHeapScanPlanState : public AbstractScanPlanState {

};

struct IndexOnlyScanPlanState : public AbstractScanPlanState {

  int ioss_NumScanKeys;
  ScanKey  ioss_ScanKeys;

};

struct MaterialPlanState : public AbstractPlanState {

};

struct LimitPlanState : public AbstractPlanState {

  int64 limit;
  int64 offset;

};

struct ResultPlanState : public AbstractPlanState {

};

struct AbstractJoinPlanState : public AbstractPlanState {

  ProjectionInfo *ps_ProjInfo;
  TupleDesc tts_tupleDescriptor;

  JoinType jointype;
  List *joinqual;
  List *qual;
};

struct NestLoopPlanState : public AbstractJoinPlanState {

};

}  // namespace planner
}  // namespace peloton
