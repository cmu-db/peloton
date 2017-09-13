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

#include "common/logger.h"
#include "common/macros.h"
#include "expression/expression_util.h"
#include "util/hash_util.h"

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

const AbstractPlan *AbstractPlan::GetChild(uint32_t child_index) const {
  PL_ASSERT(child_index < children_.size());
  return children_[child_index].get();
}

const AbstractPlan *AbstractPlan::GetParent() const { return parent_; }

// Get a string representation of this plan
std::ostream &operator<<(std::ostream &os, const AbstractPlan &plan) {
  os << PlanNodeTypeToString(plan.GetPlanNodeType());

  return os;
}

const std::string AbstractPlan::GetInfo() const {
  std::ostringstream os;
  os << PlanNodeTypeToString(GetPlanNodeType())
     << " [NumChildren=" << children_.size() << "]";
  return os.str();
}

void AbstractPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in all child plans of %s",
            GetInfo().c_str());
  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

hash_t AbstractPlan::Hash() const {
  hash_t hash = 0;
  for (auto &child : GetChildren()) {
    hash = HashUtil::CombineHashes(hash, child->Hash());
  }
  return hash;
}

bool AbstractPlan::operator==(const AbstractPlan &rhs) const {
  auto num = GetChildren().size();
  if (num != rhs.GetChildren().size())
    return false;
  for (unsigned int i = 0; i < num; i++) {
    if (*GetChild(i) != *(AbstractPlan *)rhs.GetChild(i))
      return false;
  }
  return true;
}

}  // namespace planner
}  // namespace peloton
