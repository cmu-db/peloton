/* @brief Helper routines for printing PlanState node in Postgres
 * pprint.c
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 *
 */
#include "postgres.h"

#include <ctype.h>
#include <cassert>

#include "executor/execdesc.h"
#include "foreign/fdwapi.h"
#include "nodes/pprint.h"

/* static helpers */

static void PrintPlan(FILE *DEST,
                      const PlanState *planstate,
                      const char * relationship,
                      const char * plan_name,
                      int ind);

static void PrintSubPlan(FILE *DEST,
                         List *plans,
                         const char *plan_name,
                         int ind);

static void PrintMember(FILE *DEST,
                        List *plans,
                        PlanState **planstates,
                        int ind);

static void PrintModifyTableInfo(FILE *DEST, ModifyTableState *mtstate,
                                   int ind);

/* deprecated */

static void PrintPlanState(FILE *DEST, const PlanState *planstate, int ind);

static void PrintList(FILE *DEST, const List* list, int ind);

/* Utils */
static void IndentHelper(FILE *DEST, int ind);

void PrintQueryDesc(const QueryDesc *queryDesc) {
  PrintPlan(stdout, queryDesc->planstate, NULL, NULL, 0);
}

void PrintPlanStateTree(const PlanState *planstate) {
  PrintPlan(stdout, planstate, NULL, NULL, 0);
  fprintf(stdout, "\n");
}

static void PrintPlan(FILE *DEST, const PlanState *planstate,
                       const char * relationship, const char * plan_name,
                       int ind) {
  Plan *plan = planstate->plan;

  assert(plan != nullptr);

  const char* pname;  // node type for text output
  const char* sname;
  const char* operation = NULL;
  const char* strategy = NULL;
  const char* custom_name = NULL;

  //bool haschildren;

  /* 1. Plan Type */
  switch (nodeTag(plan)) {
    case T_Result:
      pname = sname = "Result";
      break;
    case T_ModifyTable:
      sname = "ModifyTable";
      switch (((ModifyTable *) plan)->operation) {
        case CMD_INSERT:
          pname = operation = "ModifyTable:Insert";
          break;
        case CMD_UPDATE:
          pname = operation = "ModifyTable:Update";
          break;
        case CMD_DELETE:
          pname = operation = "ModifyTalbe:Delete";
          break;
        default:
          pname = "ModifyTalbe:???";
          break;
      }
      break;
    case T_Append:
      pname = sname = "Append";
      break;
    case T_MergeAppend:
      pname = sname = "Merge Append";
      break;
    case T_RecursiveUnion:
      pname = sname = "Recursive Union";
      break;
    case T_BitmapAnd:
      pname = sname = "BitmapAnd";
      break;
    case T_BitmapOr:
      pname = sname = "BitmapOr";
      break;
    case T_NestLoop:
      pname = sname = "Nested Loop";
      break;
    case T_MergeJoin:
      pname = "Merge"; /* "Join" gets added by jointype switch */
      sname = "Merge Join";
      break;
    case T_HashJoin:
      pname = "Hash"; /* "Join" gets added by jointype switch */
      sname = "Hash Join";
      break;
    case T_SeqScan:
      pname = sname = "Seq Scan";
      break;
    case T_IndexScan:
      pname = sname = "Index Scan";
      break;
    case T_IndexOnlyScan:
      pname = sname = "Index Only Scan";
      break;
    case T_BitmapIndexScan:
      pname = sname = "Bitmap Index Scan";
      break;
    case T_BitmapHeapScan:
      pname = sname = "Bitmap Heap Scan";
      break;
    case T_TidScan:
      pname = sname = "Tid Scan";
      break;
    case T_SubqueryScan:
      pname = sname = "Subquery Scan";
      break;
    case T_FunctionScan:
      pname = sname = "Function Scan";
      break;
    case T_ValuesScan:
      pname = sname = "Values Scan";
      break;
    case T_CteScan:
      pname = sname = "CTE Scan";
      break;
    case T_WorkTableScan:
      pname = sname = "WorkTable Scan";
      break;
    case T_ForeignScan:
      pname = sname = "Foreign Scan";
      break;
    case T_CustomScan:
      sname = "Custom Scan";
      custom_name = ((CustomScan *) plan)->methods->CustomName;
      if (custom_name)
        pname = psprintf("Custom Scan (%s)", custom_name);
      else
        pname = sname;
      break;
    case T_SampleScan: {
      /*TODO: If sample scan is considered in Peloton,
       * We need tablesample method and its parameter
       * The specified tablesample method can be fetched from RTE
       * */
      pname = sname = "Sample Scan";
    }
      break;
    case T_Material:
      pname = sname = "Materialize";
      break;
    case T_Sort:
      pname = sname = "Sort";
      break;
    case T_Group:
      pname = sname = "Group";
      break;
    case T_Agg:
      sname = "Aggregate";
      switch (((Agg *) plan)->aggstrategy) {
        case AGG_PLAIN:
          pname = "Aggregate";
          strategy = "Plain";
          break;
        case AGG_SORTED:
          pname = "GroupAggregate";
          strategy = "Sorted";
          break;
        case AGG_HASHED:
          pname = "HashAggregate";
          strategy = "Hashed";
          break;
        default:
          pname = "Aggregate ???";
          strategy = "???";
          break;
      }
      break;
    case T_WindowAgg:
      pname = sname = "WindowAgg";
      break;
    case T_Unique:
      pname = sname = "Unique";
      break;
    case T_SetOp:
      sname = "SetOp";
      switch (((SetOp *) plan)->strategy) {
        case SETOP_SORTED:
          pname = "SetOp";
          strategy = "Sorted";
          break;
        case SETOP_HASHED:
          pname = "HashSetOp";
          strategy = "Hashed";
          break;
        default:
          pname = "SetOp ???";
          strategy = "???";
          break;
      }
      break;
    case T_LockRows:
      pname = sname = "LockRows";
      break;
    case T_Limit:
      pname = sname = "Limit";
      break;
    case T_Hash:
      pname = sname = "Hash";
      break;
    default:
      pname = sname = "???";
      break;
  }

  IndentHelper(DEST, ind);
  fprintf(DEST, "Node Type: %s/%s|", sname, pname);  // node type
  if (plan_name) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Subplan Name: %s|", plan_name);
  }
  if (relationship) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Relationship: %s|", relationship);
  }
  if (strategy) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Strategy: %s|", strategy);
  }
  if (operation) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Operation: %s|", operation);
  }
  if (custom_name) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Custom Plan Provider: %s|", custom_name);
  }

  /* 2.
   * Scan Target
   * Scan Index
   * Modify Target
   * Join Type
   * Set command */

  switch (nodeTag(plan)) {
    case T_SeqScan:
    case T_BitmapHeapScan:
    case T_TidScan:
    case T_SubqueryScan:
    case T_FunctionScan:
    case T_ValuesScan:
    case T_CteScan:
    case T_WorkTableScan:
      /* TODO: Scan target */
      break;
    case T_ForeignScan:
    case T_CustomScan:
      /* TODO: Scan target */
      //if (((Scan *) plan)->scanrelid > 0)
      break;
    case T_SampleScan:
      /* TODO: Scan target */
      break;
    case T_IndexScan: {
      /* TODO: Index Info and Scan target */
    }
      break;
    case T_IndexOnlyScan: {
      /* TODO: Index Info and Scan target */
    }
      break;
    case T_BitmapIndexScan: {
      /* TODO: Index Info and Scan target */
    }
      break;
    case T_ModifyTable:
      /* TODO: Modify target
       * ModifyTableTarget, for relation that was named in the original query
       * ModifyTable_info(multiple target table or non-nominal relation, FDW and foreign , on conflict clause)
       * */
      PrintModifyTableInfo(DEST, (ModifyTableState *) planstate, ind + 1);

      break;
    case T_NestLoop:
    case T_MergeJoin:
    case T_HashJoin: {
      const char *jointype;

      switch (((Join *) plan)->jointype) {
        case JOIN_INNER:
          jointype = "Inner";
          break;
        case JOIN_LEFT:
          jointype = "Left";
          break;
        case JOIN_FULL:
          jointype = "Full";
          break;
        case JOIN_RIGHT:
          jointype = "Right";
          break;
        case JOIN_SEMI:
          jointype = "Semi";
          break;
        case JOIN_ANTI:
          jointype = "Anti";
          break;
        default:
          jointype = "???";
          break;
      }

      IndentHelper(DEST, ind + 1);
      fprintf(DEST, "Type: %s|", jointype);
    }
      break;
    case T_SetOp: {
      const char *setopcmd;

      switch (((SetOp *) plan)->cmd) {
        case SETOPCMD_INTERSECT:
          setopcmd = "Intersect";
          break;
        case SETOPCMD_INTERSECT_ALL:
          setopcmd = "Intersect All";
          break;
        case SETOPCMD_EXCEPT:
          setopcmd = "Except";
          break;
        case SETOPCMD_EXCEPT_ALL:
          setopcmd = "Except All";
          break;
        default:
          setopcmd = "???";
          break;
      }
      IndentHelper(DEST, ind + 1);
      fprintf(DEST, "Command: %s", setopcmd);
    }
      break;
    default:
      break;
  }

  /* 3. Target List
   * TODO: Target List goes here
   * plan->targetlist; */

  /* 4. Keys and Qualifiers
   * TODO: Qualifiers
   *       Sort key
   *       Agg key
   *       Group key
   *       goes here */

  /* 5. Child plan
   *
   * */
  /*
   haschildren = planstate->initPlan || outerPlanState(planstate)
   || innerPlanState(planstate) || IsA(plan, ModifyTable)
   || IsA(plan, Append) || IsA(plan, MergeAppend) || IsA(plan, BitmapAnd)
   || IsA(plan, BitmapOr) || IsA(plan, SubqueryScan) || planstate->subPlan;
   */
  /* initPlan(s) */
  if (planstate->initPlan) {
    PrintSubPlan(DEST, planstate->initPlan, "InitPlan", ind + 1);
  }

  /* left tree */
  if (outerPlanState(planstate)) {
    PrintPlan(DEST, outerPlanState(planstate), "LeftTree", NULL, ind + 1);
  }

  /* right tree */
  if (innerPlanState(planstate)) {
    PrintPlan(DEST, innerPlanState(planstate), "RightTree", NULL, ind + 1);

  }

  /* special child plans */
  switch (nodeTag(plan)) {
    case T_ModifyTable:
      PrintMember(DEST, ((ModifyTable *) plan)->plans,
                   ((ModifyTableState *) planstate)->mt_plans, ind + 1);
      break;
    case T_Append:
      PrintMember(DEST, ((Append *) plan)->appendplans,
                   ((AppendState *) planstate)->appendplans, ind + 1);
      break;
    case T_MergeAppend:
      PrintMember(DEST, ((MergeAppend *) plan)->mergeplans,
                   ((MergeAppendState *) planstate)->mergeplans, ind + 1);
      break;
    case T_BitmapAnd:
      PrintMember(DEST, ((BitmapAnd *) plan)->bitmapplans,
                   ((BitmapAndState *) planstate)->bitmapplans, ind + 1);
      break;
    case T_BitmapOr:
      PrintMember(DEST, ((BitmapOr *) plan)->bitmapplans,
                   ((BitmapOrState *) planstate)->bitmapplans, ind + 1);
      break;
    case T_SubqueryScan:
      PrintPlan(DEST, ((SubqueryScanState *) planstate)->subplan, "Subquery",
      NULL,
                 ind + 1);
      break;
    default:
      break;
  }

  /* subPlan-s */
  if (planstate->subPlan)
    PrintSubPlan(DEST, planstate->subPlan, "SubPlan", ind + 1);

}

static void PrintSubPlan(FILE *DEST, List *plans, const char *relationship,
                          int ind) {
  ListCell *lst;

  foreach(lst, plans)
  {
    SubPlanState *sps = (SubPlanState *) lfirst(lst);
    SubPlan *sp = (SubPlan *) sps->xprstate.expr;
    PrintPlan(DEST, sps->planstate, relationship, sp->plan_name, ind);
  }
}

static void PrintMember(FILE *DEST, List *plans, PlanState **planstates,
                         int ind) {
  int nplans = list_length(plans);
  int j;
  for (j = 0; j < nplans; ++j) {
    PrintPlan(DEST, planstates[j], "Member", NULL, ind);
  }
}

static void PrintModifyTableInfo(FILE *DEST, ModifyTableState *mtstate,
                                   int ind) {
  ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
  bool labeltargets;
  int j;

  labeltargets =
      (mtstate->mt_nplans > 1
          || (mtstate->mt_nplans == 1
              && mtstate->resultRelInfo->ri_RangeTableIndex
                  != node->nominalRelation));
  if (labeltargets) {
    IndentHelper(DEST, ind);
    fprintf(
        DEST,
        "More than one target relations or the target relation is not nominal|");
  }

  for (j = 0; j < mtstate->mt_nplans; ++j) {
    ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
    FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

    if (fdwroutine && fdwroutine->ExplainForeignModify != NULL) {
      IndentHelper(DEST, ind);
      fprintf(DEST, "Foreign modify|");
    }
  }

  if (node->onConflictAction != ONCONFLICT_NONE) {
    IndentHelper(DEST, ind);
    fprintf(DEST, "ON CONFLICT ACTION|");
  }

}

static void PrintPlanState(FILE *DEST, const PlanState *planstate, int ind) {
  IndentHelper(DEST, ind);
  if (planstate == NULL) {
    fprintf(DEST, "Plan: NULL|");
  } else {
    fprintf(DEST, "Plan: %p", (void *) planstate);

    switch (nodeTag(planstate)) {
      case T_PlanState:
        fprintf(DEST, "Plan State|");
        break;
      case T_ResultState:
        fprintf(DEST, "Result State|");
        break;
      case T_ModifyTableState:
        fprintf(DEST, "Modify Table State|");
        break;
      case T_AppendState:
        fprintf(DEST, "Append State|");
        break;
      case T_MergeAppendState:
        fprintf(DEST, "Merge Append State|");
        break;
      case T_RecursiveUnionState:
        fprintf(DEST, "Recursive Union State|");
        break;
      case T_BitmapAndState:
        fprintf(DEST, "Bitmap And State|");
        break;
      case T_BitmapOrState:
        fprintf(DEST, "Bitmap Or State|");
        break;
      case T_ScanState:
        fprintf(DEST, "Scan State|");
        break;
      case T_SeqScanState:
        fprintf(DEST, "Seq Scan State|");
        break;
      case T_SampleScanState:
        fprintf(DEST, "Sample Scan State|");
        break;
      case T_IndexScanState:
        fprintf(DEST, "Index Scan State|");
        break;
      case T_IndexOnlyScanState:
        fprintf(DEST, "Index Only Scan State|");
        break;
      case T_BitmapIndexScanState:
        fprintf(DEST, "Bitmap Index Scan State|");
        break;
      case T_BitmapHeapScanState:
        fprintf(DEST, "Bitmap Heap Scan State|");
        break;
      case T_TidScanState:
        fprintf(DEST, "Tid Scan State|");
        break;
      case T_SubqueryScanState:
        fprintf(DEST, "Subquery Scan State|");
        break;
      case T_FunctionScanState:
        fprintf(DEST, "Function Scan State|");
        break;
      case T_ValuesScanState:
        fprintf(DEST, "Values Scan State|");
        break;
      case T_CteScanState:
        fprintf(DEST, "Cte Scan State|");
        break;
      case T_WorkTableScanState:
        fprintf(DEST, "WorkTableScanState|");
        break;
      case T_ForeignScanState:
        fprintf(DEST, "Foreign Scan State|");
        break;
      case T_CustomScanState:
        fprintf(DEST, "Custom Scan State|");
        break;
      case T_JoinState:
        fprintf(DEST, "Join State|");
        break;
      case T_NestLoopState:
        fprintf(DEST, "Nest Loop State|");
        break;
      case T_MergeJoinState:
        fprintf(DEST, "Merge Join State|");
        break;
      case T_HashJoinState:
        fprintf(DEST, "Hash Join State|");
        break;
      case T_MaterialState:
        fprintf(DEST, "Material State|");
        break;
      case T_SortState:
        fprintf(DEST, "Sort State|");
        break;
      case T_GroupState:
        fprintf(DEST, "Group State|");
        break;
      case T_AggState:
        fprintf(DEST, "Agg State|");
        break;
      case T_WindowAggState:
        fprintf(DEST, "Window Agg State|");
        break;
      case T_UniqueState:
        fprintf(DEST, "Unique State|");
        break;
      case T_HashState:
        fprintf(DEST, "Hash State|");
        break;
      case T_SetOpState:
        fprintf(DEST, "Set Op State|");
        break;
      case T_LockRowsState:
        fprintf(DEST, "Lock Rows State|");
        break;
      case T_LimitState:
        fprintf(DEST, "Limit State|");
        break;
      default:
        fprintf(DEST, "No such Plan State|");
        break;
    }

    PrintList(DEST, planstate->subPlan, ind + 1);

    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Left Child:|");
    PrintPlanState(DEST, planstate->lefttree, ind + 2);

    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Right Child:|");
    PrintPlanState(DEST, planstate->righttree, ind + 2);
  }
}

static void PrintList(FILE *DEST, const List* list, int ind) {
  ListCell *l;
  IndentHelper(DEST, ind);
  fprintf(DEST, "Subplan State List: |");
  if (list == NIL) {
    IndentHelper(DEST, ind + 1);
    fprintf(DEST, "Empty List|");
  } else {
    foreach(l, list)
    {
      SubPlanState *subplanstate = (SubPlanState *) lfirst(l);
      PrintPlanState(DEST, subplanstate->planstate, ind + 1);
    }
  }
}

static void IndentHelper(FILE *DEST, int ind) {
  int i;
  for (i = 0; i < ind; i++) {
    //fprintf(DEST, "-");
  }
}

