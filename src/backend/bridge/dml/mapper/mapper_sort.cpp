//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_sort.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_sort.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/order_by_plan.h"

namespace peloton {
namespace bridge {

std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformSort(
    const SortPlanState *plan_state) {
  auto sort = plan_state->sort;

  int numCols = sort->numCols;
  AttrNumber *sortColIdx = sort->sortColIdx;
  bool *reverse_flags = plan_state->reverse_flags;

  std::vector<oid_t> sort_keys;
  std::vector<bool> descend_flags;
  std::vector<oid_t> output_col_ids;

  for (int i = 0; i < numCols; i++) {
    LOG_INFO("Sort col idx : %u , reverse : %u", sortColIdx[i],
             reverse_flags[i]);
    sort_keys.push_back(
        static_cast<oid_t>(AttrNumberGetAttrOffset(sortColIdx[i])));
    descend_flags.push_back(reverse_flags[i]);
  }

  std::unique_ptr<planner::AbstractPlan> retval(
      new planner::OrderByPlan(sort_keys, descend_flags, output_col_ids));

  auto lchild = TransformPlan(outerAbstractPlanState(plan_state));
  retval->AddChild(std::move(lchild));

  return retval;
}
}
}
