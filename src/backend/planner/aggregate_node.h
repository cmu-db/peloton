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

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_AGGREGATE;
  }

  const std::vector<oid_t>& GetAggregateColumnsOffsets() const {
    return aggregate_columns_offsets;
  }

  const std::vector<oid_t>& GetGroupByColumnsOffsets() const {
    return group_by_columns_offsets;
  }

  const std::map<oid_t, oid_t>& GetPassThroughColumnsMapping() const {
    return pass_through_columns_mapping;
  }

  const std::vector<ExpressionType>& GetAggregateTypes() const {
    return aggregate_types;
  }

  const catalog::Schema *GetOutputTableSchema() const {
    return output_table_schema;
  }

  const catalog::Schema *GetGroupBySchema() const {
    return group_by_key_schema;
  }

 private:

  /** @brief Aggregate columns */
  std::vector<oid_t> aggregate_columns_offsets;

  /** @brief Group by columns */
  std::vector<oid_t> group_by_columns_offsets;

  /** @brief Aggregate column schema */
  catalog::Schema *group_by_key_schema;

  /** @brief Pass through columns */
  std::map<oid_t, oid_t> pass_through_columns_mapping;

  /** @brief Aggregate types */
  std::vector<ExpressionType> aggregate_types;

  /** @brief Output columns */
  catalog::Schema *output_table_schema;

};

} // namespace planner
} // namespace nstore

