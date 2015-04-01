/**
 * @brief Header for materialization plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <vector>

#include "common/types.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {

class MaterializationNode : public AbstractPlanNode {
 public:
  /**
   * @brief Returns ordering of column ids to be used for materialization.
   *
   * @return Vector of column ids.
   */
  const std::vector<id_t>& column_ids() const {
    return column_ids_;
  }

 private:
  /**
   * @brief Defines order of column ids after materialization.
   *
   * Reshuffling of column ids is handled by this node instead of the
   * projection node. TODO is this a good idea?
   */
  std::vector<id_t> column_ids_;

} // namespace planner
} // namespace nstore
