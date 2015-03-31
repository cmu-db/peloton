/**
 * @brief Header for projection plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {
    
class ProjectionNode : public AbstractPlanNode {
 public:
  /**
   * @brief Return output column ids.
   * 
   * @return Vector of output column ids.
   */
  const std::vector<id_t>& output_column_ids() const {
    return output_column_ids_;
  }

 private:
  /** 
   * @brief Column ids to output after projection.
   *
   * TODO For now we only store column ids. We need to import the
   * whole expression system from voltdb to support parameterized plans and
   * expressions..
   */
  std::vector<id_t> output_column_ids_;
};

} // namespace planner
} // namespace nstore
