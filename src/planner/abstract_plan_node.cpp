/**
 * @brief Base class for all plan nodes.
 *
 * Copyright(c) 2015, CMU
 */

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/types.h"
#include "common/logger.h"
#include "planner/abstract_plan_node.h"
#include "planner/plan_column.h"
#include "planner/plan_node_util.h"

namespace nstore {
namespace planner {

AbstractPlanNode::AbstractPlanNode(oid_t plan_node_id)
: plan_node_id(plan_node_id) {
}

AbstractPlanNode::AbstractPlanNode()
: plan_node_id(-1){
}

AbstractPlanNode::~AbstractPlanNode() {
  // clean up inlined nodes
  for (auto node : inlined_nodes){
    delete node.second;
  }
}

//===--------------------------------------------------------------------===//
// Children + Parent Helpers
//===--------------------------------------------------------------------===//

void AbstractPlanNode::AddChild(AbstractPlanNode* child) {
  children.push_back(child);
}

std::vector<AbstractPlanNode*>& AbstractPlanNode::GetChildren() {
  return children;
}

std::vector<oid_t>& AbstractPlanNode::GetChildrenIds(){
  return children_ids;
}

const std::vector<AbstractPlanNode*>& AbstractPlanNode::GetChildren() const {
  return children;
}

void AbstractPlanNode::AddParent(AbstractPlanNode* parent){
  parents.push_back(parent);
}

std::vector<AbstractPlanNode*>& AbstractPlanNode::GetParents(){
  return parents;
}

std::vector<oid_t>& AbstractPlanNode::GetParentIds() {
  return parent_ids;
}

const std::vector<AbstractPlanNode*>& AbstractPlanNode::GetParents() const {
  return parents;
}

//===--------------------------------------------------------------------===//
// Inlined plannodes
//===--------------------------------------------------------------------===//

void AbstractPlanNode::AddInlinePlanNode(AbstractPlanNode* inline_node) {
  inlined_nodes[inline_node->GetPlanNodeType()] = inline_node;
  inline_node->is_inlined = true;
}

AbstractPlanNode* AbstractPlanNode::GetInlinePlanNodes(PlanNodeType type) const {

  auto lookup = inlined_nodes.find(type);
  AbstractPlanNode* ret = nullptr;

  if (lookup != inlined_nodes.end()) {
    ret = lookup->second;
  }
  else {
    LOG_TRACE("No internal PlanNode with type : " << PlanNodeToString(type)
                  << " is available for " << debug() );
  }

  return ret;
}

std::map<PlanNodeType, AbstractPlanNode*>& AbstractPlanNode::GetInlinePlanNodes() {
  return inlined_nodes;
}

const std::map<PlanNodeType, AbstractPlanNode*>& AbstractPlanNode::GetInlinePlanNodes() const{
  return inlined_nodes;
}

bool AbstractPlanNode::IsInlined() const {
  return is_inlined;
}

//===--------------------------------------------------------------------===//
// Accessors
//===--------------------------------------------------------------------===//

void AbstractPlanNode::SetPlanNodeId(oid_t plan_node_id_) {
  plan_node_id = plan_node_id_;
}

oid_t AbstractPlanNode::GetPlanNodeId() const {
  return plan_node_id;
}

void AbstractPlanNode::SetExecutor(executor::AbstractExecutor* executor_) {
  executor = executor_;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this plan node
std::ostream& operator<<(std::ostream& os, const AbstractPlanNode& node) {
  os << node.debug();
  return os;
}

std::string AbstractPlanNode::debug() const {
  std::ostringstream buffer;

  buffer << PlanNodeToString(this->GetPlanNodeType())
      << "[" << this->GetPlanNodeId() << "]";

  return buffer.str();
}

std::string AbstractPlanNode::debug(bool traverse) const {
  return (traverse ? this->debug(std::string("")) : this->debug());
}

std::string AbstractPlanNode::debug(const std::string& spacer) const {
  std::ostringstream buffer;
  buffer << spacer << "* " << this->debug() << "\n";
  std::string info_spacer = spacer + "  |";
  buffer << this->debugInfo(info_spacer);

  // Inline PlanNodes
  if (!inlined_nodes.empty()) {

    buffer << info_spacer << "Inlined Plannodes: "
        << inlined_nodes.size() << "\n";
    std::string internal_spacer = info_spacer + "  ";
    for (auto node : inlined_nodes) {
      buffer << info_spacer << "Inline "
          << PlanNodeToString(node.second->GetPlanNodeType())
          << ":\n";
      buffer << node.second->debugInfo(internal_spacer);
    }
  }

  // Traverse the tree
  std::string child_spacer = spacer + "  ";
  for (int ctr = 0, cnt = static_cast<int>(children.size()); ctr < cnt; ctr++) {
    buffer << child_spacer << children[ctr]->GetPlanNodeType() << "\n";
    buffer << children[ctr]->debug(child_spacer);
  }
  return (buffer.str());
}

} // namespace planner
} // namespace nstore
