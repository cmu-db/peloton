/**
 * @brief Base class for all plan nodes.
 *
 * Copyright(c) 2015, CMU
 */

#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {

/**
 * @brief Virtual destructor to make this class polymorphic.
 * 
 * We want to be able to downcast plan nodes using dynamic_cast.
 */
AbstractPlanNode::~AbstractPlanNode() {}

/**
 * @brief Add child node to abstract plan node.
 * @param child Child node to add to this plan node.
 */
void AbstractPlanNode::AddChild(AbstractPlanNode *child) {
  children_.push_back(child);
}

/**
 * @brief Returns all children nodes of this node.
 *
 * @return Vector of children nodes.
 */
const std::vector<AbstractPlanNode *>& AbstractPlanNode::children() const {
  return children_;
}

} // namespace planner
} // namespace nstore
