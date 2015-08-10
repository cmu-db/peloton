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
#include "backend/common/logger.h"
#include "backend/planner/abstract_plan.h"

#include "nodes/execnodes.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

peloton::planner::AbstractPlanState *
DMLUtils::BuildPlanState(peloton::planner::AbstractPlanState *root,
                         PlanState *planstate){
  // Base case
  if (planstate == nullptr) return root;

  peloton::planner::AbstractPlanState *child_planstate = nullptr;

  auto planstate_type = nodeTag(planstate);
  switch (planstate_type) {

    case T_ModifyTableState:
      break;

    default:
      elog(ERROR, "unrecognized node type: %d", planstate_type);
      break;
  }

  // Base case
  if (child_planstate != nullptr) {
    if (root != nullptr)
      root->AddChild(child_planstate);
    else
      root = child_planstate;
  }

  // Recurse
  auto left_child = outerPlanState(planstate);
  auto right_child = innerPlanState(planstate);

  if(left_child)
    BuildPlanState(child_planstate, left_child);
  if(right_child)
    BuildPlanState(child_planstate, right_child);

  return root;
}

/**
 * @brief preparing data
 * @param planstate
 */
peloton::planner::AbstractPlanState *
DMLUtils::peloton_prepare_data(PlanState *planstate) {

  auto peloton_planstate = BuildPlanState(nullptr, planstate);

  return peloton_planstate;
}

}  // namespace bridge
}  // namespace peloton
