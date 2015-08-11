//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.cpp
//
// Identification: src/backend/bridge/ddl/ddl_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "backend/bridge/dml/mapper/dml_utils.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/logger.h"

#include "postgres.h"
#include "nodes/execnodes.h"
#include "common/fe_memutils.h"
#include "utils/rel.h"

extern Datum ExecEvalExprSwitchContext(ExprState *expression,
                                       ExprContext *econtext, bool *isNull,
                                       ExprDoneCond *isDone);

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

ExprState *CopyExprState(ExprState *expr_state);

AbstractPlanState *
DMLUtils::PreparePlanState(AbstractPlanState *root,
                           PlanState *planstate,
                           bool left_child){
  // Base case
  if (planstate == nullptr) return root;

  AbstractPlanState *child_planstate = nullptr;

  auto planstate_type = nodeTag(planstate);
  switch (planstate_type) {

    case T_ModifyTableState:
      child_planstate = PrepareModifyTableState(
          reinterpret_cast<ModifyTableState *>(planstate));
      break;
    case T_SeqScanState:
      child_planstate = PrepareSeqScanState(
          reinterpret_cast<SeqScanState *>(planstate));
      break;
    case T_IndexScanState:
      child_planstate = PrepareIndexScanState(
          reinterpret_cast<IndexScanState *>(planstate));
      break;
    case T_IndexOnlyScanState:
      child_planstate = PrepareIndexOnlyScanState(
          reinterpret_cast<IndexOnlyScanState *>(planstate));
      break;
    case T_BitmapHeapScanState:
      child_planstate = PrepareBitmapHeapScanState(
          reinterpret_cast<BitmapHeapScanState *>(planstate));
      break;
    case T_LockRowsState:
      child_planstate = PrepareLockRowsState(
          reinterpret_cast<LockRowsState *>(planstate));
      break;
    case T_LimitState:
      child_planstate = PrepareLimitState(
          reinterpret_cast<LimitState *>(planstate));
      break;

    case T_MaterialState:
      child_planstate = PrepareMaterialState(
          reinterpret_cast<MaterialState *>(planstate));
      break;

    case T_MergeJoinState:
    case T_HashJoinState:
    case T_NestLoopState:
      child_planstate = PrepareNestLoopState(
          reinterpret_cast<NestLoopState *>(planstate));
      break;

    default:
      elog(ERROR, "unrecognized node type: %d", planstate_type);
      break;
  }

  // Base case
  if (child_planstate != nullptr) {
    if (root != nullptr) {
      if(left_child)
        outerAbstractPlanState(root) = child_planstate;
      else
        innerAbstractPlanState(root) = child_planstate;
    }
    else
      root = child_planstate;
  }

  // Recurse
  auto left_tree = outerPlanState(planstate);
  auto right_tree = innerPlanState(planstate);

  if(left_tree)
    PreparePlanState(child_planstate, left_tree, true);
  if(right_tree)
    PreparePlanState(child_planstate, right_tree, false);

  return root;
}

ModifyTablePlanState *
DMLUtils::PrepareModifyTableState(ModifyTableState *mt_plan_state) {

  ModifyTablePlanState *info = (ModifyTablePlanState*) palloc(sizeof(ModifyTablePlanState));
  info->type = mt_plan_state->ps.type;

  // Resolve result table
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;
  oid_t table_nattrs = result_relation_desc->rd_att->natts;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  info->operation = mt_plan_state->operation;
  info->database_oid = database_oid;
  info->table_oid = table_oid;
  info->table_nattrs = table_nattrs;

  switch (info->operation) {
    case CMD_INSERT:
      LOG_INFO("CMD_INSERT");
      PrepareInsertState(info, mt_plan_state);
      break;

    case CMD_UPDATE:
      LOG_INFO("CMD_UPDATE");
      PrepareUpdateState(info, mt_plan_state);
      break;

    case CMD_DELETE:
      LOG_INFO("CMD_DELETE");
      PrepareDeleteState(info, mt_plan_state);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", info->operation);
      return nullptr;
      break;
  }

  return info;
}

void
DMLUtils::PrepareInsertState(ModifyTablePlanState *info,
                             ModifyTableState *mt_plan_state) {

  // Should be only one sub plan which is a Result
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  // Child is a result node
  if (nodeTag(sub_planstate->plan) == T_Result) {
    LOG_TRACE("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState *>(sub_planstate);

    // We only handle single-constant-tuple for now,
    // i.e., ResultState should have no children/sub plans
    assert(outerPlanState(result_ps) == nullptr);

    auto child_planstate = PrepareResultState(reinterpret_cast<ResultState *>(sub_planstate));
    child_planstate->proj = BuildProjectInfo(result_ps->ps.ps_ProjInfo,
                                             info->table_nattrs);

    info->mt_plans = (AbstractPlanState **) palloc(sizeof(AbstractPlanState *) *
                                                   mt_plan_state->mt_nplans);
    info->mt_plans[0] = child_planstate;
  }
  else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }

}

void
DMLUtils::PrepareUpdateState(ModifyTablePlanState *info,
                             ModifyTableState *mt_plan_state) {
  // Should be only one sub plan which is a SeqScan
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  // Get the first sub plan state
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];
  assert(sub_planstate);

  auto child_tag = nodeTag(sub_planstate->plan);

  if (child_tag == T_SeqScan || child_tag == T_IndexScan ||
      child_tag == T_IndexOnlyScan ||
      child_tag == T_BitmapHeapScan) {  // Sub plan is a Scan of any type

    LOG_TRACE("Child of Update is %u \n", child_tag);

    // Extract the projection info from the underlying scan
    // and put it in our update node
    auto scan_state = reinterpret_cast<ScanState *>(sub_planstate);

    auto child_planstate = (AbstractScanPlanState *)
            PreparePlanState(nullptr, sub_planstate, true);

    child_planstate->proj = BuildProjectInfo(scan_state->ps.ps_ProjInfo,
                                             info->table_nattrs);

    info->mt_plans = (AbstractPlanState **) palloc(sizeof(AbstractPlanState *) *
                                                   mt_plan_state->mt_nplans);
    info->mt_plans[0] = child_planstate;

  } else {
    LOG_ERROR("Unsupported sub plan type of Update : %u \n", child_tag);
  }


}

void
DMLUtils::PrepareDeleteState(ModifyTablePlanState *info,
                             ModifyTableState *mt_plan_state) {

  // Grab Database ID and Table ID
  // Input must come from a subplan
  assert(mt_plan_state->resultRelInfo);
  // Maybe relax later. I don't know when they can have >1 subplans.
  assert(mt_plan_state->mt_nplans == 1);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  auto child_planstate = PreparePlanState(nullptr, sub_planstate, true);

  info->mt_plans = (AbstractPlanState **) palloc(sizeof(AbstractPlanState *) *
                                                 mt_plan_state->mt_nplans);
  info->mt_plans[0] = child_planstate;

}


ResultPlanState *
DMLUtils::PrepareResultState(ResultState *result_plan_state) {
  ResultPlanState *info = (ResultPlanState*) palloc(sizeof(ResultPlanState));
  info->type = result_plan_state->ps.type;

  return info;
}

LockRowsPlanState *
DMLUtils::PrepareLockRowsState(LockRowsState *lr_plan_state) {
  LockRowsPlanState *info = (LockRowsPlanState*) palloc(sizeof(LockRowsPlanState));
  info->type = lr_plan_state->ps.type;

  PlanState *outer_plan_state = outerPlanState(lr_plan_state);
  AbstractPlanState *child_plan_state = PreparePlanState(nullptr, outer_plan_state, true);
  outerAbstractPlanState(info) = child_plan_state;

  return info;
}

LimitPlanState *
DMLUtils::PrepareLimitState(LimitState *limit_plan_state) {
  LimitPlanState *info = (LimitPlanState*) palloc(sizeof(LimitPlanState));
  info->type = limit_plan_state->ps.type;

  ExprContext *econtext = limit_plan_state->ps.ps_ExprContext;
  Datum val;
  bool isNull;
  int64 limit;
  int64 offset;
  bool noLimit;
  bool noOffset;

  /* Resolve limit and offset */
  if (limit_plan_state->limitOffset) {
    val = ExecEvalExprSwitchContext(limit_plan_state->limitOffset,
                                    econtext, &isNull, NULL);
    /* Interpret NULL offset as no offset */
    if (isNull)
      offset = 0;
    else {
      offset = DatumGetInt64(val);
      if (offset < 0) {
        LOG_ERROR("OFFSET must not be negative, offset = %ld", offset);
      }
      noOffset = false;
    }
  } else {
    /* No OFFSET supplied */
    offset = 0;
  }

  if (limit_plan_state->limitCount) {
    val = ExecEvalExprSwitchContext(limit_plan_state->limitCount,
                                    econtext, &isNull, NULL);
    /* Interpret NULL count as no limit (LIMIT ALL) */
    if (isNull) {
      limit = 0;
      noLimit = true;
    } else {
      limit = DatumGetInt64(val);
      if (limit < 0) {
        LOG_ERROR("LIMIT must not be negative, limit = %ld", limit);
      }
      noLimit = false;
    }
  } else {
    /* No LIMIT supplied */
    limit = 0;
    noLimit = true;
  }

  info->limit = limit;
  info->offset = offset;
  info->noLimit = noLimit;
  info->noOffset = noOffset;

  PlanState *outer_plan_state = outerPlanState(limit_plan_state);
  AbstractPlanState *child_plan_state = PreparePlanState(nullptr, outer_plan_state, true);
  outerAbstractPlanState(info) = child_plan_state;

  return info;
}

NestLoopPlanState *
DMLUtils::PrepareNestLoopState(NestLoopState *nl_plan_state){
  NestLoopPlanState *info = (NestLoopPlanState*) palloc(sizeof(NestLoopPlanState));
  info->type = nl_plan_state->js.ps.type;

  const JoinState *js = &(nl_plan_state->js);
  info->jointype = js->jointype;

  // Copy join qual expr states
  auto qual_list = js->joinqual;
  ListCell *qual_item;
  info->joinqual = NIL;
  foreach(qual_item, qual_list) {
    ExprState *expr_state = (ExprState *)lfirst(qual_item);
    ExprState *expr_state_copy = CopyExprState(expr_state);
    info->joinqual = lappend(info->joinqual, expr_state_copy);
  }

  // Copy ps qual expr states
  auto ps_qual_list = js->ps.qual;
  info->qual = NIL;
  foreach(qual_item, ps_qual_list) {
    ExprState *expr_state = (ExprState *)lfirst(qual_item);
    ExprState *expr_state_copy = CopyExprState(expr_state);
    info->qual = lappend(info->qual, expr_state_copy);
  }

  // Copy tuple desc
  auto tup_desc = js->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
  info->tts_tupleDescriptor = CreateTupleDescCopy(tup_desc);

  // Construct projection info
  info->ps_ProjInfo = BuildProjectInfo(js->ps.ps_ProjInfo,
                                       tup_desc->natts);

  return info;
}


void
DMLUtils::PrepareAbstractScanState(AbstractScanPlanState *ss_plan_state,
                                   ScanState *ss_state) {

  // Resolve table
  Relation ss_relation_desc = ss_state->ss_currentRelation;
  ss_plan_state->table_oid = ss_relation_desc->rd_id;

  // Copy expr states
  auto qual_list = ss_state->ps.qual;
  ListCell *qual_item;

  ss_plan_state->qual = NIL;
  foreach(qual_item, qual_list) {
    ExprState *expr_state = (ExprState *)lfirst(qual_item);

    ExprState *expr_state_copy = CopyExprState(expr_state);
    ss_plan_state->qual = lappend(ss_plan_state->qual, expr_state_copy);
  }

  // Copy tuple desc
  auto tup_desc = ss_state->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
  ss_plan_state->tts_tupleDescriptor = CreateTupleDescCopy(tup_desc);

}

SeqScanPlanState *
DMLUtils::PrepareSeqScanState(SeqScanState *ss_plan_state) {
  SeqScanPlanState *info = (SeqScanPlanState*) palloc(sizeof(SeqScanPlanState));
  info->type = ss_plan_state->ps.type;

  // First, build the abstract scan state
  PrepareAbstractScanState(info, ss_plan_state);

  // Resolve table
  Relation ss_relation_desc = ss_plan_state->ss_currentRelation;
  oid_t table_nattrs = ss_relation_desc->rd_att->natts;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = ss_relation_desc->rd_id;

  info->database_oid = database_oid;
  info->table_oid = table_oid;
  info->table_nattrs = table_nattrs;

  return info;
}


MaterialPlanState *
DMLUtils::PrepareMaterialState(MaterialState *material_plan_state) {
  MaterialPlanState *info = (MaterialPlanState*) palloc(sizeof(MaterialPlanState));
  info->type = material_plan_state->ss.ps.type;

  PlanState *outer_plan_state = outerPlanState(material_plan_state);
  AbstractPlanState *child_plan_state = PreparePlanState(nullptr, outer_plan_state, true);
  outerAbstractPlanState(info) = child_plan_state;

  return info;
}

IndexScanPlanState *
DMLUtils::PrepareIndexScanState(IndexScanState *iss_plan_state) {
  IndexScanPlanState *info = (IndexScanPlanState*) palloc(sizeof(IndexScanPlanState));
  info->type = iss_plan_state->ss.ps.type;


  return info;
}

IndexOnlyScanPlanState *
DMLUtils::PrepareIndexOnlyScanState(IndexOnlyScanState *ioss_plan_state) {
  IndexOnlyScanPlanState *info = (IndexOnlyScanPlanState*) palloc(sizeof(IndexOnlyScanPlanState));
  info->type = ioss_plan_state->ss.ps.type;


  return info;
}

BitmapHeapScanPlanState *
DMLUtils::PrepareBitmapHeapScanState(BitmapHeapScanState *bhss_plan_state) {
  BitmapHeapScanPlanState *info = (BitmapHeapScanPlanState*) palloc(sizeof(BitmapHeapScanPlanState));
  info->type = bhss_plan_state->ss.ps.type;


  return info;
}


/**
 * @brief preparing data
 * @param planstate
 */
AbstractPlanState *
DMLUtils::peloton_prepare_data(PlanState *planstate) {

  auto peloton_planstate = PreparePlanState(nullptr, planstate, false);

  return peloton_planstate;
}

PelotonProjectionInfo *
DMLUtils::BuildProjectInfo(ProjectionInfo *pg_pi, int column_count){
  PelotonProjectionInfo *info = (PelotonProjectionInfo*) palloc(sizeof(PelotonProjectionInfo));

  info->expr_states = NIL;
  // (A) Transform non-trivial target list
  ListCell *tl;
  foreach (tl, pg_pi->pi_targetlist) {
    GenericExprState *gstate = (GenericExprState *)lfirst(tl);
    TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;
    AttrNumber resind = tle->resno - 1;

    if (!(resind < column_count && AttributeNumberIsValid(tle->resno) &&
        AttrNumberIsForUserDefinedAttr(tle->resno) && !tle->resjunk)) {
      LOG_TRACE("Invalid / Junk attribute. Skipped.");
      continue;  // skip junk attributes
    }

    oid_t col_id = static_cast<oid_t>(resind);
    auto expr_state = CopyExprState(gstate->arg);

    info->expr_states = lappend(info->expr_states, expr_state);
    info->expr_col_ids = lappend_int(info->expr_col_ids, col_id);

  }

  //  (B) Transform direct map list
  // Special case:
  // a null constant may be specified in SimpleVars by PG,
  // in case of that, we add a Target to target_list we created above.

  info->out_col_ids = NIL;
  info->tuple_idxs = NIL;
  info->in_col_ids = NIL;

  if (pg_pi->pi_numSimpleVars > 0) {
    int numSimpleVars = pg_pi->pi_numSimpleVars;
    int *varSlotOffsets = pg_pi->pi_varSlotOffsets;
    int *varNumbers = pg_pi->pi_varNumbers;

    if (pg_pi->pi_directMap)  // Sequential direct map
    {
      /* especially simple case where vars go to output in order */
      for (int i = 0; i < numSimpleVars && i < column_count; i++) {
        oid_t tuple_idx =
            (varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1
                : 0);
        int varNumber = varNumbers[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(i);

        info->out_col_ids = lappend_int(info->out_col_ids, out_col_id);
        info->tuple_idxs = lappend_int(info->tuple_idxs, tuple_idx);
        info->in_col_ids = lappend_int(info->in_col_ids, in_col_id);

        LOG_TRACE("Input column : %u , Output column : %u", in_col_id,
                  out_col_id);
      }
    } else  // Non-sequential direct map
    {
      /* we have to pay attention to varOutputCols[] */
      int *varOutputCols = pg_pi->pi_varOutputCols;

      for (int i = 0; i < numSimpleVars; i++) {
        oid_t tuple_idx =
            (varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1
                : 0);
        int varNumber = varNumbers[i] - 1;
        int varOutputCol = varOutputCols[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(varOutputCol);

        info->out_col_ids = lappend_int(info->out_col_ids, out_col_id);
        info->tuple_idxs = lappend_int(info->tuple_idxs, tuple_idx);
        info->in_col_ids = lappend_int(info->in_col_ids, in_col_id);

        LOG_TRACE("Input column : %u , Output column : %u \n", in_col_id,
                  out_col_id);
      }
    }
  }

  return info;
}

ExprState *CopyExprState(ExprState *expr_state){

  ExprState *expr_state_copy = nullptr;

  // Copy children as well
  switch(nodeTag(expr_state)){
    case T_BoolExprState:
    {
      expr_state_copy = (ExprState *) makeNode(BoolExprState);
      BoolExprState *bool_expr_state_copy = (BoolExprState *) expr_state_copy;
      BoolExprState *bool_expr_state =  (BoolExprState *) expr_state;
      List       *items = bool_expr_state->args;
      ListCell   *item;

      bool_expr_state_copy->args = NIL;
      foreach(item, items) {
        ExprState  *expr_state = (ExprState *) lfirst(item);
        auto child_expr_state = CopyExprState(expr_state);
        bool_expr_state_copy->args = lappend(bool_expr_state_copy->args,
                                             child_expr_state);
      }
    }
    break;

    case T_FuncExprState:
    {
      expr_state_copy = (ExprState *) makeNode(FuncExprState);
      FuncExprState *func_expr_state_copy = (FuncExprState *) expr_state_copy;
      FuncExprState *func_expr_state =  (FuncExprState *) expr_state;
      List       *items = func_expr_state->args;
      ListCell   *item;

      func_expr_state_copy->args = NIL;
      foreach(item, items) {
        ExprState  *expr_state = (ExprState *) lfirst(item);
        auto child_expr_state = CopyExprState(expr_state);
        func_expr_state_copy->args = lappend(func_expr_state_copy->args,
                                             child_expr_state);
      }
    }
    break;

    default:
    {
      expr_state_copy = (ExprState *) makeNode(ExprState);
    }
    break;
  }

  expr_state_copy->type = expr_state->type;
  expr_state_copy->expr = (Expr *) copyObject(expr_state->expr);

  return expr_state_copy;
}


}  // namespace bridge
}  // namespace peloton
