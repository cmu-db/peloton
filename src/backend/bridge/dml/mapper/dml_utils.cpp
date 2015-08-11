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

#include "nodes/execnodes.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

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

/**
 * @brief preparing data
 * @param planstate
 */
AbstractPlanState *
DMLUtils::peloton_prepare_data(PlanState *planstate) {

  auto peloton_planstate = BuildPlanState(nullptr, planstate, false);

  return peloton_planstate;
}

}  // namespace bridge
}  // namespace peloton
