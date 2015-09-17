//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// plan_node_util.h
//
// Identification: src/backend/planner/plan_node_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "abstract_plan.h"

namespace peloton {
namespace planner {

AbstractPlan *GetEmptyPlanNode(PlanNodeType type);

}  // namespace planner
}  // namespace peloton
