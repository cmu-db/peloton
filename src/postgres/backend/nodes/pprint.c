/* @brief Helper routines for printing PlanState node in Postgres
 * pprint.c
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 *
 */
#include "postgres.h"

#include <ctype.h>
#include "nodes/pprint.h"

static void print_planstate(const PlanState *planstate, int ind);
static void print_list(const List* list, int ind);
static void indent(int ind);

#define DEST stdout

void printQueryDesc(const QueryDesc *queryDesc) {
  print_planstate(queryDesc->planstate, 0);
}

void printPlanStateTree(const PlanState *planstate) {
  print_planstate(planstate, 0);
}

static void print_planstate(const PlanState *planstate, int ind) {
  indent(ind);
  if (planstate == NULL) {
    fprintf(DEST, "Void\n");

  } else {
    fprintf(DEST, "Plan: ");

    switch (nodeTag(planstate)) {
      case T_PlanState:
        fprintf(DEST, "Plan State\n");
        break;
      case T_ResultState:
        fprintf(DEST, "Result State\n");
        break;
      case T_ModifyTableState:
        fprintf(DEST, "Modify Table State\n");
        break;
      case T_AppendState:
        fprintf(DEST, "Append State\n");
        break;
      case T_MergeAppendState:
        fprintf(DEST, "Merge Append State\n");
        break;
      case T_RecursiveUnionState:
        fprintf(DEST, "Recursive Union State\n");
        break;
      case T_BitmapAndState:
        fprintf(DEST, "BItmap And State\n");
        break;
      case T_BitmapOrState:
        fprintf(DEST, "Bitmap Or State\n");
        break;
      case T_ScanState:
        fprintf(DEST, "Scan State\n");
        break;
      case T_SeqScanState:
        fprintf(DEST, "Seq Scan State\n");
        break;
      case T_SampleScanState:
        fprintf(DEST, "Sample Scan State\n");
        break;
      case T_IndexScanState:
        fprintf(DEST, "Index Scan State\n");
        break;
      case T_IndexOnlyScanState:
        fprintf(DEST, "Index Only Scan State\n");
        break;
      case T_BitmapIndexScanState:
        fprintf(DEST, "Bitmap Index Scan State\n");
        break;
      case T_BitmapHeapScanState:
        fprintf(DEST, "Bitmap Heap Scan State\n");
        break;
      case T_TidScanState:
        fprintf(DEST, "Tid Scan State\n");
        break;
      case T_SubqueryScanState:
        fprintf(DEST, "Subquery Scan State\n");
        break;
      case T_FunctionScanState:
        fprintf(DEST, "Function Scan State\n");
        break;
      case T_ValuesScanState:
        fprintf(DEST, "Values Scan State\n");
        break;
      case T_CteScanState:
        fprintf(DEST, "Cte Scan State\n");
        break;
      case T_WorkTableScanState:
        fprintf(DEST, "WorkTableScanState\n");
        break;
      case T_ForeignScanState:
        fprintf(DEST, "Foreign Scan State\n");
        break;
      case T_CustomScanState:
        fprintf(DEST, "Custom Scan State\n");
        break;
      case T_JoinState:
        fprintf(DEST, "Join State\n");
        break;
      case T_NestLoopState:
        fprintf(DEST, "Nest Loop State\n");
        break;
      case T_MergeJoinState:
        fprintf(DEST, "Merge Join State\n");
        break;
      case T_HashJoinState:
        fprintf(DEST, "Hash Join State\n");
        break;
      case T_MaterialState:
        fprintf(DEST, "Material State\n");
        break;
      case T_SortState:
        fprintf(DEST, "Sort State\n");
        break;
      case T_GroupState:
        fprintf(DEST, "Group State\n");
        break;
      case T_AggState:
        fprintf(DEST, "Agg State\n");
        break;
      case T_WindowAggState:
        fprintf(DEST, "Window Agg State\n");
        break;
      case T_UniqueState:
        fprintf(DEST, "Unique State\n");
        break;
      case T_HashState:
        fprintf(DEST, "Hash State\n");
        break;
      case T_SetOpState:
        fprintf(DEST, "Set Op State\n");
        break;
      case T_LockRowsState:
        fprintf(DEST, "Lock Rows State\n");
        break;
      case T_LimitState:
        fprintf(DEST, "Limit State\n");
        break;
      default:
        fprintf(DEST, "No such Plan State\n");
        break;
    }

    print_list(planstate->state->es_subplanstates, ind + 1);

    indent(ind + 1);
    fprintf(DEST, "Left Child:\n");
    print_planstate(planstate->lefttree, ind + 2);

    indent(ind + 1);
    fprintf(DEST, "Right Child:\n");
    print_planstate(planstate->righttree, ind + 2);
  }
}

static void print_list(const List* list, int ind) {
  ListCell *l;
  indent(ind);
  fprintf(DEST, "Subplan State List: \n");
  if (list == NIL) {
    indent(ind + 1);
    fprintf(DEST, "Empty List\n");
  } else {
    foreach(l, list)
    {
      PlanState *planstate = (PlanState *) lfirst(l);
      print_planstate(planstate, ind + 1);
    }
  }
}

static void indent(int ind) {
  int i;
  for (i = 0; i < ind; i++) {
    fprintf(DEST, "\t");
  }
}

