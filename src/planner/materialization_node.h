/**
 * @brief Header for materialization plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {

class MaterializationNode : public AbstractPlanNode {
 public:
  /**
   * @brief Returns mapping of old column ids to new column ids after
   *        materialization.
   *
   * @return Unordered map of old column ids to new column ids.
   */
  std::unordered_map<id_t, id_t> &old_to_new_cols() {
    return old_to_new_cols_;
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
   * @brief Mapping of old column ids to new column ids after materialization.
   */
  std::unordered_map<id_t, id_t> old_to_new_cols_;

  /** @brief Names of the respective columns. */
  std::vector<std::string> column_names_;

  /** @brief Schema of newly materialized tile. */
  catalog::Schema schema_;
};

} // namespace planner
} // namespace nstore
