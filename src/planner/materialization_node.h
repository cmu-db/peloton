/**
 * @brief Header for materialization plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "planner/abstract_plan_node.h"

namespace nstore {

namespace catalog {
class Schema;
}

namespace planner {

class MaterializationNode : public AbstractPlanNode {
 public:
  MaterializationNode(const MaterializationNode &) = delete;
  MaterializationNode& operator=(const MaterializationNode &) = delete;
  MaterializationNode(MaterializationNode &&) = delete;
  MaterializationNode& operator=(MaterializationNode &&) = delete;

  MaterializationNode(
      std::unordered_map<id_t, id_t> &&old_to_new_cols,
      catalog::Schema *schema);

  const std::unordered_map<id_t, id_t>& old_to_new_cols() const;

  const catalog::Schema& schema() const;

  PlanNodeType GetPlanNodeType() const;

  std::string debugInfo(const std::string& spacer) const;

 private:
  /**
   * @brief Mapping of old column ids to new column ids after materialization.
   */
  std::unordered_map<id_t, id_t> old_to_new_cols_;

  /** @brief Schema of newly materialized tile. */
  std::unique_ptr<catalog::Schema> schema_;
};

} // namespace planner
} // namespace nstore
