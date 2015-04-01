/**
 * @brief Header for projection plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <unordered_set>

#include "common/types.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {
    
class ProjectionNode : public AbstractPlanNode {
 public:
  /**
   * @brief Return output column ids.
   * 
   * @return Set of output column ids.
   */
  inline const std::unordered_set<id_t>& output_column_ids() const {
    return output_column_ids_;
  }

 private:
  /** @brief Set of output column ids. */
  std::unordered_set<id_t> output_column_ids_;
};

} // namespace planner
} // namespace nstore
