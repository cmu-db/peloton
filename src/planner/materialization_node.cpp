/**
 * @brief Implementation of materialization plan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "planner/materialization_node.h"

#include <utility>

#include "catalog/schema.h"

namespace nstore {
namespace planner {

MaterializationNode::MaterializationNode(
    std::vector<AbstractPlanNode *> &&children,
    std::unordered_map<id_t, id_t> &&old_to_new_cols,
    std::vector<std::string> &&column_names,
    catalog::Schema *schema)
  : AbstractPlanNode(std::move(children)),
    old_to_new_cols_(std::move(old_to_new_cols)),
    column_names_(std::move(column_names)),
    schema_(schema) {
}

/**
 * @brief Returns mapping of old column ids to new column ids after
 *        materialization.
 *
 * @return Unordered map of old column ids to new column ids.
 */
std::unordered_map<id_t, id_t> &MaterializationNode::old_to_new_cols() {
  return old_to_new_cols_;
}

/**
 * @brief Returns names of columns.
 *
 * @return Vector of names.
 */
const std::vector<std::string> &MaterializationNode::column_names() const {
  return column_names_;
}

/**
 * @brief Returns schema of newly materialized tile.
 *
 * @return Schema of newly materialized tile.
 */
const catalog::Schema &MaterializationNode::schema() const {
  return *schema_;
}


} // namespace planner
} // namespace nstore
