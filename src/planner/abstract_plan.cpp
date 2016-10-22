//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/planner/abstract_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/abstract_plan.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/logger.h"
#include "common/types.h"
#include "expression/expression_util.h"

namespace peloton {
namespace planner {

AbstractPlan::AbstractPlan() {}

AbstractPlan::~AbstractPlan() {}

void AbstractPlan::AddChild(std::unique_ptr<AbstractPlan> &&child) {
  children_.emplace_back(std::move(child));
}

const std::vector<std::unique_ptr<AbstractPlan>> &AbstractPlan::GetChildren()
    const {
  return children_;
}

const AbstractPlan *AbstractPlan::GetParent() { return parent_; }

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
    os << child_spacer << children_[ctr].get()->GetPlanNodeType() << "\n";
    os << children_[ctr].get()->GetInfo();
  }

  return os.str();
}

void AbstractPlan::SetParameterValues(std::vector<common::Value> *values) {
  LOG_TRACE("Setting parameter values in all child plans of %s",
            GetInfo().c_str());
  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
};

}  // namespace planner
}  // namespace peloton
