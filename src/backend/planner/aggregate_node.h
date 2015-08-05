//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// aggregate_node.h
//
// Identification: src/backend/planner/aggregate_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "backend/planner/abstract_plan_node.h"
#include "backend/common/types.h"

#include <map>

namespace peloton {
namespace planner {

// IMPORTANT:: Need own copy of output table schema.
// TODO: Can we relax this constraint ?
class AggregateNode : public AbstractPlanNode {
 public:
  AggregateNode() = delete;
  AggregateNode(const AggregateNode&) = delete;
  AggregateNode& operator=(const AggregateNode&) = delete;
  AggregateNode(AggregateNode&&) = delete;
  AggregateNode& operator=(AggregateNode&&) = delete;

  AggregateNode(const std::vector<oid_t>& aggregate_columns,
                const std::map<oid_t, oid_t>& aggregate_columns_map,
                const std::vector<oid_t>& group_by_columns,
                const catalog::Schema* group_by_key_schema,
                const std::map<oid_t, oid_t>& pass_through_columns_map,
                const std::vector<ExpressionType>& aggregate_types,
                catalog::Schema* output_table_schema)
      : aggregate_columns_(aggregate_columns),
        aggregate_columns_map_(aggregate_columns_map),
        group_by_columns_(group_by_columns),
        group_by_key_schema_(group_by_key_schema),
        pass_through_columns_map_(pass_through_columns_map),
        aggregate_types_(aggregate_types),
        output_table_schema_(output_table_schema) {}

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::PLAN_NODE_TYPE_AGGREGATE;
  }

  const std::vector<oid_t>& GetAggregateColumns() const {
    return aggregate_columns_;
  }

  const std::map<oid_t, oid_t>& GetAggregateColumnsMap() const {
    return aggregate_columns_map_;
  }

  const std::vector<oid_t>& GetGroupByColumns() const {
    return group_by_columns_;
  }

  const catalog::Schema* GetGroupByKeySchema() const {
    return group_by_key_schema_;
  }

  const std::map<oid_t, oid_t>& GetPassThroughColumnsMap() const {
    return pass_through_columns_map_;
  }

  const std::vector<ExpressionType>& GetAggregateTypes() const {
    return aggregate_types_;
  }

  catalog::Schema* GetOutputTableSchema() const { return output_table_schema_; }

 private:
  /** @brief Aggregate columns */
  const std::vector<oid_t> aggregate_columns_;

  /** @brief Aggregate columns mapping */
  const std::map<oid_t, oid_t> aggregate_columns_map_;

  /** @brief Group by columns */
  const std::vector<oid_t> group_by_columns_;

  /** @brief Group by key tuple used (Needed only for hash aggregation) */
  const catalog::Schema* group_by_key_schema_;

  /** @brief Pass through columns mapping (input -> output) */
  const std::map<oid_t, oid_t> pass_through_columns_map_;

  /** @brief Aggregate types */
  const std::vector<ExpressionType> aggregate_types_;

  /** @brief Output columns */
  catalog::Schema* output_table_schema_;
};

}  // namespace planner
}  // namespace peloton
