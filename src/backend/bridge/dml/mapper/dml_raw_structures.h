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

#include "postgres.h"
#include "c.h"
#include "nodes/execnodes.h"

//===--------------------------------------------------------------------===//
// DDL raw data structures
//===--------------------------------------------------------------------===//

struct AbstractPlanState {
  NodeTag type;

  AbstractPlanState *left_tree;
  AbstractPlanState *right_tree;
};

struct ModifyTablePlanState : public AbstractPlanState {

  CmdType operation;
  Oid database_oid;
  Oid table_oid;

  int table_nattrs;

  AbstractPlanState **mt_plans;   /* subplans (one per target rel) */

};

struct LockRowsPlanState : public AbstractPlanState {

};

struct AbstractScanPlanState : public AbstractPlanState {

  Oid table_oid;

  //ProjectionInfo *ps_ProjInfo;
  TupleDesc tts_tupleDescriptor;

  List *qual;

};

struct SeqScanPlanState : public AbstractScanPlanState {

};

struct IndexScanPlanState : public AbstractScanPlanState {

  int iss_NumScanKeys;
  //ScanKey   iss_ScanKeys;

  Oid indexid;
  //ScanDirection indexorderdir;
};

struct BitmapHeapScanPlanState : public AbstractScanPlanState {

};

struct IndexOnlyScanPlanState : public AbstractScanPlanState {

  int ioss_NumScanKeys;
  //ScanKey  ioss_ScanKeys;

  Oid indexid;
  //ScanDirection indexorderdir;

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

  //ProjectionInfo *ps_ProjInfo;
  TupleDesc tts_tupleDescriptor;

  JoinType jointype;
  List *joinqual;
  List *qual;
};

struct NestLoopPlanState : public AbstractJoinPlanState {

};

