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

#include <map>

namespace nstore {
namespace planner {

class AggregateNode : public AbstractPlanNode {
 public:
  AggregateNode() = delete;
  AggregateNode(const AggregateNode &) = delete;
  AggregateNode& operator=(const AggregateNode &) = delete;
  AggregateNode(AggregateNode &&) = delete;
  AggregateNode& operator=(AggregateNode &&) = delete;

  AggregateNode(const std::vector<oid_t>& aggregate_columns)
  : aggregate_columns_(aggregate_columns) {
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_AGGREGATE;
  }

  const std::vector<oid_t>& GetAggregateColumns() const {
    return aggregate_columns_;
  }

  const std::vector<oid_t>& GetGroupByColumns() const {
    return group_by_columns_;
  }

  const std::map<oid_t, oid_t>& GetPassThroughColumns() const {
    return pass_through_columns_;
  }

  const std::vector<ExpressionType>& GetAggregateTypes() const {
    return aggregate_types_;
  }

  const catalog::Schema *GetOutputTableSchema() const {
    return output_table_schema_;
  }

  const catalog::Schema *GetGroupBySchema() const {
    return group_by_key_schema_;
  }

 private:

  /** @brief Aggregate columns */
  const std::vector<oid_t> aggregate_columns_;

  /** @brief Group by columns */
  const std::vector<oid_t> group_by_columns_;

  /** @brief Aggregate column schema */
  const catalog::Schema *group_by_key_schema_;

  /** @brief Pass through columns */
  const std::map<oid_t, oid_t> pass_through_columns_;

  /** @brief Aggregate types */
  const std::vector<ExpressionType> aggregate_types_;

  /** @brief Output columns */
  const catalog::Schema *output_table_schema_;

};

} // namespace planner
} // namespace nstore

