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

  AggregateNode(const std::vector<oid_t>& aggregate_columns,
                const std::map<oid_t, oid_t>& aggregate_columns_mapping,
                const std::vector<oid_t>& group_by_columns,
                const std::map<oid_t, oid_t>& pass_through_columns,
                const std::vector<ExpressionType>& aggregate_types,
                const catalog::Schema *output_table_schema)
  : aggregate_columns_(aggregate_columns),
    aggregate_columns_map_(aggregate_columns_mapping),
    group_by_columns_(group_by_columns),
    pass_through_columns_map_(pass_through_columns),
    aggregate_types_(aggregate_types),
    output_table_schema_(output_table_schema){
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_AGGREGATE;
  }

  const std::vector<oid_t>& GetAggregateColumns() const {
    return aggregate_columns_;
  }

  const std::map<oid_t, oid_t>& GetAggregateColumnsMapping() const {
    return aggregate_columns_map_;
  }

  const std::vector<oid_t>& GetGroupByColumns() const {
    return group_by_columns_;
  }

  const std::map<oid_t, oid_t>& GetPassThroughColumnsMapping() const {
    return pass_through_columns_map_;
  }

  const std::vector<ExpressionType>& GetAggregateTypes() const {
    return aggregate_types_;
  }

  const catalog::Schema *GetOutputTableSchema() const {
    return output_table_schema_;
  }

 private:

  /** @brief Aggregate columns */
  const std::vector<oid_t> aggregate_columns_;

  /** @brief Aggregate columns mapping */
  const std::map<oid_t, oid_t> aggregate_columns_map_;

  /** @brief Group by columns */
  const std::vector<oid_t> group_by_columns_;

  /** @brief Pass through columns mapping (input -> output) */
  const std::map<oid_t, oid_t> pass_through_columns_map_;

  /** @brief Aggregate types */
  const std::vector<ExpressionType> aggregate_types_;

  /** @brief Output columns */
  const catalog::Schema *output_table_schema_;

};

} // namespace planner
} // namespace nstore

