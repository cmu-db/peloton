#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/order_by_node.h"

#include "utils/tuplesort.h"

namespace peloton {
namespace bridge {

planner::AbstractPlanNode*
PlanTransformer::TransformSort(const SortState *plan_state) {
  auto sort_state = plan_state;
  auto sort = reinterpret_cast<const Sort*>(plan_state->ss.ps.plan);

  int numCols = sort->numCols;
  AttrNumber* sortColIdx = sort->sortColIdx;

  for (int i = 0; i < numCols; i++) {
    LOG_INFO("Sort Col Idx : %d, Sort OperatorOid : %u", sortColIdx[i],
             sort->sortOperators[i]);
  }

  (void) sort_state;

  auto lchild = TransformPlan(outerPlanState(sort_state));

  // return the child temporarily
  return lchild;

}

}
}
