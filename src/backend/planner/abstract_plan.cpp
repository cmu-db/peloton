//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_plan_node.cpp
//
// Identification: src/backend/planner/abstract_plan_node.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
#include "plan_util.h"

namespace peloton {
namespace planner {

AbstractPlan::AbstractPlan(oid_t plan_node_id)
    : plan_node_id_(plan_node_id) {}

AbstractPlan::AbstractPlan() : plan_node_id_(-1) {}

AbstractPlan::~AbstractPlan() {}

//===--------------------------------------------------------------------===//
// Children + Parent Helpers
//===--------------------------------------------------------------------===//

void AbstractPlan::AddChild(AbstractPlan *child) {
  children_.push_back(child);
}

const std::vector<AbstractPlan *> &AbstractPlan::GetChildren() const {
  return children_;
}

AbstractPlan *AbstractPlan::GetParent() { return parent_; }

//===--------------------------------------------------------------------===//
// Accessors
//===--------------------------------------------------------------------===//

void AbstractPlan::SetPlanNodeId(oid_t plan_node_id) {
  plan_node_id_ = plan_node_id;
}

oid_t AbstractPlan::GetPlanNodeId() const { return plan_node_id_; }

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this plan node
std::ostream &operator<<(std::ostream &os, const AbstractPlan &node) {
  os << PlanNodeTypeToString(node.GetPlanNodeType());
  os << "[" << node.GetPlanNodeId() << "]";

  return os;
}

std::string AbstractPlan::GetInfo(std::string spacer) const {
  std::ostringstream buffer;
  buffer << spacer << "* " << this->GetInfo() << "\n";
  std::string info_spacer = spacer + "  |";
  buffer << this->GetInfo(info_spacer);

  // Traverse the tree
  std::string child_spacer = spacer + "  ";
  for (int ctr = 0, cnt = static_cast<int>(children_.size()); ctr < cnt;
       ctr++) {
    buffer << child_spacer << children_[ctr]->GetPlanNodeType() << "\n";
    buffer << children_[ctr]->GetInfo(child_spacer);
  }
  return (buffer.str());
}

std::string AbstractPlan::GetInfo() const { return ""; }

}  // namespace planner
}  // namespace peloton
