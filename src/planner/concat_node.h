/**
 * @brief Header for concat plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <string>
#include <vector>

#include "planner/abstract_plan_node.h"
#include "common/types.h"

namespace nstore {
namespace planner {

class ConcatNode : public AbstractPlanNode {
 public:
  /** @brief Stores the metadata for new column to be added. */
  struct ColumnPointer {
    /** @brief Position list that will correspond to this column. */
    unsigned int position_list_idx;

    /** @brief Id of base tile corresponding to this column. */
    oid_t base_tile_oid;

    /** @brief Original column id of this column in the base tile. */
    id_t origin_column_id;
  };

  ConcatNode(const ConcatNode &) = delete;
  ConcatNode& operator=(const ConcatNode &) = delete;
  ConcatNode(ConcatNode &&) = delete;
  ConcatNode& operator=(ConcatNode &&) = delete;

  ConcatNode(std::vector<ColumnPointer> &&new_columns);

  inline const std::vector<ColumnPointer>& new_columns() const {
    return new_columns_;
  }

  PlanNodeType GetPlanNodeType() const;

  std::string debugInfo(const std::string& spacer) const;

 private:
  /** @brief Vector of new columns to add to logical tile. */
  std::vector<ColumnPointer> new_columns_;
};

} // namespace planner
} // namespace nstore
