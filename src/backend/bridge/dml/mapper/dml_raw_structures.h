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
#include "access/skey.h"

//===--------------------------------------------------------------------===//
// DDL raw data structures
//===--------------------------------------------------------------------===//

struct AbstractPlanState {
  NodeTag type;

  AbstractPlanState *left_tree;
  AbstractPlanState *right_tree;
};

/* ----------------
 *  these are defined to avoid confusion problems with "left"
 *  and "right" and "inner" and "outer".  The convention is that
 *  the "left" plan is the "outer" plan and the "right" plan is
 *  the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerAbstractPlanState(node)    (((AbstractPlanState *)(node))->right_tree)
#define outerAbstractPlanState(node)    (((AbstractPlanState *)(node))->left_tree)

struct PelotonProjectionInfo {

  List *expr_states;
  List *expr_col_ids;

  List *out_col_ids;
  List *tuple_idxs;
  List *in_col_ids;

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
  Oid database_oid;

  TupleDesc tts_tupleDescriptor;

  List *qual; // expr states

  PelotonProjectionInfo* proj;
};

struct SeqScanPlanState : public AbstractScanPlanState {

  int table_nattrs;

};

struct IndexScanPlanState : public AbstractScanPlanState {

  IndexScan *iss_plan;

  ScanKey   iss_ScanKeys;
  int iss_NumScanKeys;
};

struct BitmapHeapScanPlanState : public AbstractScanPlanState {

};

struct IndexOnlyScanPlanState : public AbstractScanPlanState {

  IndexOnlyScan *ioss_plan;

  ScanKey  ioss_ScanKeys;
  int ioss_NumScanKeys;
};

struct MaterialPlanState : public AbstractPlanState {

};

struct LimitPlanState : public AbstractPlanState {

  int64 limit;
  int64 offset;
  bool noLimit;
  bool noOffset;

};

struct ResultPlanState : public AbstractPlanState {

  PelotonProjectionInfo* proj;

};

struct AbstractJoinPlanState : public AbstractPlanState {

  PelotonProjectionInfo *ps_ProjInfo;
  TupleDesc tts_tupleDescriptor;

  JoinType jointype;
  List *joinqual;
  List *qual;

};

struct NestLoopPlanState : public AbstractJoinPlanState {

};


