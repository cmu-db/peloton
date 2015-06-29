/**
 * @brief Base class for all plan nodes.
 *
 * Copyright(c) 2015, CMU
 */

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/planner/abstract_plan_node.h"
#include "backend/planner/plan_column.h"
#include "backend/planner/plan_node_util.h"

namespace peloton {
namespace planner {

AbstractPlanNode::AbstractPlanNode(oid_t plan_node_id)
    : plan_node_id_(plan_node_id) {
}

AbstractPlanNode::AbstractPlanNode()
    : plan_node_id_(-1) {
}

AbstractPlanNode::~AbstractPlanNode() {
}

//===--------------------------------------------------------------------===//
// Children + Parent Helpers
//===--------------------------------------------------------------------===//

void AbstractPlanNode::AddChild(AbstractPlanNode* child) {
    children_.push_back(child);
}

std::vector<AbstractPlanNode*>& AbstractPlanNode::GetChildren() {
    return children_;
}

AbstractPlanNode* AbstractPlanNode::GetParent() {
    return parent_;
}

//===--------------------------------------------------------------------===//
// Accessors
//===--------------------------------------------------------------------===//

void AbstractPlanNode::SetPlanNodeId(oid_t plan_node_id) {
    plan_node_id_ = plan_node_id;
}

oid_t AbstractPlanNode::GetPlanNodeId() const {
    return plan_node_id_;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

// Get a string representation of this plan node
std::ostream& operator<<(std::ostream& os, const AbstractPlanNode& node) {

    os << PlanNodeToString(node.GetPlanNodeType());
    os << "[" << node.GetPlanNodeId() << "]";

    return os;
}

std::string AbstractPlanNode::GetInfo(std::string spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "* " << this->GetInfo() << "\n";
    std::string info_spacer = spacer + "  |";
    buffer << this->GetInfo(info_spacer);

    // Traverse the tree
    std::string child_spacer = spacer + "  ";
    for (int ctr = 0, cnt = static_cast<int>(children_.size()); ctr < cnt; ctr++) {
        buffer << child_spacer << children_[ctr]->GetPlanNodeType() << "\n";
        buffer << children_[ctr]->GetInfo(child_spacer);
    }
    return (buffer.str());
}

std::string AbstractPlanNode::GetInfo() const {
    return "";
}

} // namespace planner
} // namespace peloton
