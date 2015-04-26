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
    catalog::Schema *schema)
  : old_to_new_cols_(std::move(old_to_new_cols)),
    schema_(schema) {
}

} // namespace planner
} // namespace nstore
