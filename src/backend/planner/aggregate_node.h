/*-------------------------------------------------------------------------
 *
 * aggregate_node.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/planner/aggregate_node.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/planner/abstract_plan_node.h"
#include "backend/common/types.h"

namespace nstore {
namespace planner {

class AggregateNode : public AbstractPlanNode {
 public:
  AggregateNode() = delete;
  AggregateNode(const AggregateNode &) = delete;
  AggregateNode& operator=(const AggregateNode &) = delete;
  AggregateNode(AggregateNode &&) = delete;
  AggregateNode& operator=(AggregateNode &&) = delete;

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_AGGREGATE;
  }

 private:

  /** @brief Columns in the output table */
  std::vector<catalog::ColumnInfo> output_columns;

  /** @brief Aggregate columns */
  std::vector<ExpressionType> aggregate_types;
  std::vector<catalog::ColumnInfo> aggregate_columns;

  /** @brief Group by columns */
  std::vector<catalog::ColumnInfo> group_by_columns;

};

} // namespace planner
} // namespace nstore

