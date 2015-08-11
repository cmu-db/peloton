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

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

ExprState *CopyExprState(ExprState *expr_state);

AbstractPlanState *
DMLUtils::BuildPlanState(AbstractPlanState *root,
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

    default:
      elog(ERROR, "unrecognized node type: %d", planstate_type);
      break;
  }

  // Base case
  if (child_planstate != nullptr) {
    if (root != nullptr) {
      if(left_child)
        root->left_tree = child_planstate;
      else
        root->right_tree = child_planstate;
    }
    else
      root = child_planstate;
  }

  // Recurse
  auto left_tree = outerPlanState(planstate);
  auto right_tree = innerPlanState(planstate);

  if(left_tree)
    BuildPlanState(child_planstate, left_tree, true);
  if(right_tree)
    BuildPlanState(child_planstate, right_tree, false);

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

  // Should be only one sub plan which is a Result
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  info->mt_plans = (AbstractPlanState **) palloc(sizeof(AbstractPlanState *) *
                                                 mt_plan_state->mt_nplans);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  // Child is a result node
  if (nodeTag(sub_planstate->plan) == T_Result) {
    LOG_TRACE("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState *>(sub_planstate);

    // We only handle single-constant-tuple for now,
    // i.e., ResultState should have no children/sub plans
    assert(outerPlanState(result_ps) == nullptr);

    auto child_planstate = PrepareResultState(reinterpret_cast<ResultState *>(sub_planstate));
    child_planstate->proj = BuildProjectInfo(result_ps->ps.ps_ProjInfo, table_nattrs);

    info->mt_plans[0] = child_planstate;
  }
  else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }

  return info;
}

ResultPlanState *
DMLUtils::PrepareResultState(ResultState *result_plan_state) {
  ResultPlanState *info = (ResultPlanState*) palloc(sizeof(ResultPlanState));
  info->type = result_plan_state->ps.type;

  return info;
}

/**
 * @brief preparing data
 * @param planstate
 */
AbstractPlanState *
DMLUtils::peloton_prepare_data(PlanState *planstate) {

  auto peloton_planstate = BuildPlanState(nullptr, planstate, false);

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
