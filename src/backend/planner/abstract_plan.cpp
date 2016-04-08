//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/backend/planner/abstract_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_plan.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/planner/plan_column.h"
#include "backend/planner/plan_util.h"

namespace peloton {
namespace planner {

AbstractPlan::AbstractPlan() {}

AbstractPlan::~AbstractPlan() {}

void AbstractPlan::AddChild(const AbstractPlan *child) {
  children_.push_back(child);
}

const std::vector<const AbstractPlan *> &AbstractPlan::GetChildren() const {
  return children_;
}

const AbstractPlan *AbstractPlan::GetParent() const { return parent_; }

// Get a string representation of this plan
std::ostream &operator<<(std::ostream &os, const AbstractPlan &plan) {
  os << PlanNodeTypeToString(plan.GetPlanNodeType());

  return os;
}

const std::string AbstractPlan::GetInfo() const {
  std::ostringstream os;

  os << GetInfo();

  // Traverse the tree
  std::string child_spacer = "  ";
  for (int ctr = 0, cnt = static_cast<int>(children_.size()); ctr < cnt;
       ctr++) {
    os << child_spacer << children_[ctr]->GetPlanNodeType() << "\n";
    os << children_[ctr]->GetInfo();
  }

  return os.str();
}

}  // namespace planner
}  // namespace peloton
