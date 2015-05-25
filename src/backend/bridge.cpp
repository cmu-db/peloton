/*-------------------------------------------------------------------------
 *
 * bridge.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/bridge.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge.h"
#include "common/logger.h"

#include <cassert>

/* ----------------------------------------------------------------
 *    Based on ExecProcNode in execProcnode.c
 *
 *    Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *NStoreExecute(PlanState *node){

  TupleTableSlot *result = NULL;

  fprintf(stderr, "Node type: %d \n", (int) nodeTag(node));

  switch (nodeTag(node))
  {
    /*
     * control nodes
     */
    case T_ResultState:
      break;

    case T_ModifyTableState:
      break;

    case T_AppendState:
      break;

    case T_MergeAppendState:
      break;

    case T_RecursiveUnionState:
      break;

      /* BitmapAndState does not yield tuples */

      /* BitmapOrState does not yield tuples */

      /*
       * scan nodes
       */
    case T_SeqScanState:
      break;

    case T_SampleScanState:
      break;

    case T_IndexScanState:
      break;

    case T_IndexOnlyScanState:
      break;

      /* BitmapIndexScanState does not yield tuples */

    case T_BitmapHeapScanState:
      break;

    case T_TidScanState:
      break;

    case T_SubqueryScanState:
      break;

    case T_FunctionScanState:
      break;

    case T_ValuesScanState:
      break;

    case T_CteScanState:
      break;

    case T_WorkTableScanState:
      break;

    case T_ForeignScanState:
      break;

    case T_CustomScanState:
      break;

      /*
       * join nodes
       */
    case T_NestLoopState:
      break;

    case T_MergeJoinState:
      break;

    case T_HashJoinState:
      break;

      /*
       * materialization nodes
       */
    case T_MaterialState:
      break;

    case T_SortState:
      break;

    case T_GroupState:
      break;

    case T_AggState:
      break;

    case T_WindowAggState:
      break;

    case T_UniqueState:
      break;

    case T_HashState:
      break;

    case T_SetOpState:
      break;

    case T_LockRowsState:
      break;

    case T_LimitState:
      break;

    default:
      fprintf(stderr, "unrecognized node type: %d", (int) nodeTag(node));
      result = NULL;
      break;
  }

  return result;
}
