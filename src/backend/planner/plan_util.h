//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util.h
//
// Identification: src/backend/planner/plan_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "backend/planner/abstract_plan.h"

namespace peloton {
namespace planner {

AbstractPlan *GetEmptyPlanNode(PlanNodeType type);

}  // namespace planner
}  // namespace peloton
