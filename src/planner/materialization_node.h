/**
 * @brief Header for materialization plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <string>
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
  const std::vector<id_t> &column_ids() const {
    return column_ids_;
  }

  /**
   * @brief Returns names of columns.
   *
   * @return Vector of names.
   */
   const std::vector<std::string> &column_names() const {
     return column_names_;
   }

   /**
    * @brief Returns schema of newly materialized tile.
    *
    * @return Schema of newly materialized tile.
    */
   const catalog::Schema &schema() const {
    return schema_;
   }

 private:
  /**
   * @brief Defines order of column ids after materialization.
   *
   * Reshuffling of column ids is handled by this node instead of the
   * projection node.
   */
  std::vector<id_t> column_ids_;

  /** @brief Names of the respective columns. */
  std::vector<std::string> column_names_;

  /** @brief Schema of newly materialized tile. */
  catalog::Schema schema_;
};

} // namespace planner
} // namespace nstore
