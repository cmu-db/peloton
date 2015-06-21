/*
 * transformer.cpp
 *
 *  Copyright(c) 2015, CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */
#include <iostream>
#include "planner/transformer.h"

namespace nstore {
namespace planner {

PlanTransformer&
PlanTransformer::GetInstance() {
  static PlanTransformer planTransformer;
  return planTransformer;
}

void PlanTransformer::printPostgresPlanStates(const PlanState * planstate) {
  printPostgresPlanStates(planstate);
}

void PlanTransformer::printPostgresPlanStates(const PlanState * planstate,
                                              int indentLevel) {
  indent(indentLevel);
  if (planstate == NULL) {
    std::cout << "Void" << std::endl;
  } else {
    std::cout << "Plan: ";

    switch (nodeTag(planstate)) {
      case T_PlanState:
        std::cout << "Plan State" << std::endl;
        break;
      case T_ResultState:
        std::cout << "Result State" << std::endl;
        break;
      case T_ModifyTableState:
        std::cout << "Modify Table State" << std::endl;
        break;
      case T_AppendState:
        std::cout << "Append State" << std::endl;
        break;
      case T_MergeAppendState:
        std::cout << "Merge Append State" << std::endl;
        break;
      case T_RecursiveUnionState:
        std::cout << "Recursive Union State" << std::endl;
        break;
      case T_BitmapAndState:
        std::cout << "BItmap And State" << std::endl;
        break;
      case T_BitmapOrState:
        std::cout << "Bitmap Or State" << std::endl;
        break;
      case T_ScanState:
        std::cout << "Scan State" << std::endl;
        break;
      case T_SeqScanState:
        std::cout << "Seq Scan State" << std::endl;
        break;
      case T_SampleScanState:
        std::cout << "Sample Scan State" << std::endl;
        break;
      case T_IndexScanState:
        std::cout << "Index Scan State" << std::endl;
        break;
      case T_IndexOnlyScanState:
        std::cout << "Index Only Scan State" << std::endl;
        break;
      case T_BitmapIndexScanState:
        std::cout << "Bitmap Index Scan State" << std::endl;
        break;
      case T_BitmapHeapScanState:
        std::cout << "Bitmap Heap Scan State" << std::endl;
        break;
      case T_TidScanState:
        std::cout << "Tid Scan State" << std::endl;
        break;
      case T_SubqueryScanState:
        std::cout << "Subquery Scan State" << std::endl;
        break;
      case T_FunctionScanState:
        std::cout << "Function Scan State" << std::endl;
        break;
      case T_ValuesScanState:
        std::cout << "Values Scan State" << std::endl;
        break;
      case T_CteScanState:
        std::cout << "Cte Scan State" << std::endl;
        break;
      case T_WorkTableScanState:
        std::cout << "WorkTableScanState" << std::endl;
        break;
      case T_ForeignScanState:
        std::cout << "Foreign Scan State" << std::endl;
        break;
      case T_CustomScanState:
        std::cout << "Custom Scan State" << std::endl;
        break;
      case T_JoinState:
        std::cout << "Join State" << std::endl;
        break;
      case T_NestLoopState:
        std::cout << "Nest Loop State" << std::endl;
        break;
      case T_MergeJoinState:
        std::cout << "Merge Join State" << std::endl;
        break;
      case T_HashJoinState:
        std::cout << "Hash Join State" << std::endl;
        break;
      case T_MaterialState:
        std::cout << "Material State" << std::endl;
        break;
      case T_SortState:
        std::cout << "Sort State" << std::endl;
        break;
      case T_GroupState:
        std::cout << "Group State" << std::endl;
        break;
      case T_AggState:
        std::cout << "Agg State" << std::endl;
        break;
      case T_WindowAggState:
        std::cout << "Window Agg State" << std::endl;
        break;
      case T_UniqueState:
        std::cout << "Unique State" << std::endl;
        break;
      case T_HashState:
        std::cout << "Hash State" << std::endl;
        break;
      case T_SetOpState:
        std::cout << "Set Op State" << std::endl;
        break;
      case T_LockRowsState:
        std::cout << "Lock Rows State" << std::endl;
        break;
      case T_LimitState:
        std::cout << "Limit State" << std::endl;
        break;
      default:
        std::cout << "No such Plan State" << std::endl;
        break;
    }

    printSubPlanStateList(planstate->state->es_subplanstates, indentLevel + 1);

    indent(indentLevel + 1);
    std::cout << "Left Child:" << std::endl;
    printPostgresPlanStates(planstate->lefttree, indentLevel + 2);

    indent(indentLevel + 1);
    std::cout << "Right Child:" << std::endl;
    printPostgresPlanStates(planstate->righttree, indentLevel + 2);
  }

}

void PlanTransformer::printSubPlanStateList(const List * list,
                                            int indentLevel) {
  ListCell *l;
  indent(indentLevel);  // indent it!
  std::cout << "Subplan State List:" << std::endl;

  if (list == NIL) {
    std::cout << "Empty List" << std::endl;
  } else {
    foreach(l, list)
    {
      PlanState *planstate = static_cast<PlanState *>(lfirst(l));
      printPostgresPlanStates(planstate, indentLevel + 1);
    }
  }

}

void PlanTransformer::indent(int indentLevel) {
  std::string indentation('\t', indentLevel);
  std::cout << indentation << std::endl;
}

}
}

