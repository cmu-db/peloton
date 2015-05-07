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
#include "executor/logical_tile.h"

namespace nstore {
namespace planner {

class ConcatNode : public AbstractPlanNode {
 public:
  ConcatNode(const ConcatNode &) = delete;
  ConcatNode& operator=(const ConcatNode &) = delete;
  ConcatNode(ConcatNode &&) = delete;
  ConcatNode& operator=(ConcatNode &&) = delete;

  explicit ConcatNode(const std::vector<executor::ColumnInfo> &new_columns)
    : new_columns_(new_columns) {
  }

  inline const std::vector<executor::ColumnInfo>& new_columns() const {
    return new_columns_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    //TODO Implement.
    return PLAN_NODE_TYPE_INVALID;
  }

  inline std::string GetInfo() const {
    //TODO Implement.
    return "";
  }

 private:
  /** @brief Vector of new columns to add to logical tile. */
  std::vector<executor::ColumnInfo> new_columns_;
};

} // namespace planner
} // namespace nstore
