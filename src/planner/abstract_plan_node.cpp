/**
 * @brief Base class for all plan nodes.
 *
 * Copyright(c) 2015, CMU
 */

#include "planner/abstract_plan_node.h"

#include <utility>

namespace nstore {
namespace planner {

/**
 * @brief Constructor for abstract plan node.
 * @param children Vector of child nodes of this plan node.
 */
AbstractPlanNode::AbstractPlanNode(std::vector<AbstractPlanNode *> &&children)
  : children_(std::move(children)) {
}

/**
 * @brief Virtual destructor to make this class polymorphic.
 *
 * We want to be able to downcast plan nodes using dynamic_cast.
 */
AbstractPlanNode::~AbstractPlanNode() {
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
