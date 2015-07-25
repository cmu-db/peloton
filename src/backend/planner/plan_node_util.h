/**
 * @brief Header for plan node utilities.
 *
 * Copyright(c) 2015, CMU
 *
 */

#include <string>

#include "backend/planner/abstract_plan_node.h"

namespace peloton {
namespace planner {

AbstractPlanNode* GetEmptyPlanNode(PlanNodeType type);

}  // namespace planner
}  // namespace peloton
