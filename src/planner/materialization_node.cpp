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
    std::unordered_map<id_t, id_t> &&old_to_new_cols,
    std::vector<std::string> &&column_names,
    catalog::Schema *schema)
  : m_old_to_new_cols(std::move(old_to_new_cols)),
    m_column_names(std::move(column_names)),
    m_schema(schema) {
}

std::unordered_map<id_t, id_t> &MaterializationNode::GetOldToNewCols() {
  return m_old_to_new_cols;
}

const std::vector<std::string> &MaterializationNode::GetColumnNames() const {
  return m_column_names;
}

const catalog::Schema &MaterializationNode::GetSchema() const {
  return *m_schema;
}

PlanNodeType MaterializationNode::GetPlanNodeType() const {
  //TODO Implement.
  return PLAN_NODE_TYPE_INVALID;
}

std::string MaterializationNode::debugInfo(const std::string& spacer) const {
  (void) spacer;
  //TODO Implement.
  return "";
}

} // namespace planner
} // namespace nstore
