#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/order_by_plan.h"

#include "utils/tuplesort.h"

namespace peloton {
namespace bridge {

planner::AbstractPlan*
PlanTransformer::TransformSort(const SortPlanState *plan_state){

  auto sort = plan_state->sort;

  int numCols = sort->numCols;
  AttrNumber* sortColIdx = sort->sortColIdx;

  for (int i = 0; i < numCols; i++) {
    LOG_INFO("Sort Col Idx : %d, Sort OperatorOid : %u", sortColIdx[i],
             sort->sortOperators[i]);
  }

  auto lchild = TransformPlan(outerAbstractPlanState(plan_state));
  return lchild;
//  retval->AddChild(lchild);



}

}
}
