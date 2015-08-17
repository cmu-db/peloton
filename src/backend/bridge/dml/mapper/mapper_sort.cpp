#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/order_by_plan.h"



namespace peloton {
namespace bridge {

planner::AbstractPlan*
PlanTransformer::TransformSort(const SortPlanState *plan_state) {

  auto sort = plan_state->sort;

  int numCols = sort->numCols;
  AttrNumber* sortColIdx = sort->sortColIdx;
  bool *reverse_flags = plan_state->reverse_flags;

  for(int i=0; i < numCols; i++){
    LOG_INFO("Sort Col Idx : %u , reverse : %u", sortColIdx[i], reverse_flags[i]);
  }


  auto lchild = TransformPlan(outerAbstractPlanState(plan_state));
  return lchild;
//  retval->AddChild(lchild);

}

}
}
