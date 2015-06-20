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

  /** @brief Columns in the output logical tile
   *  These are not present in input logical tile
   **/
  std::vector<catalog::ColumnInfo> output_columns;

  /**
   *  Sample queries :
   *
   *  Consider this schema:
   *  Products :: < ProductID | CategoryID | Units >
   *
   *  SELECT CategoryID, SUM(Units)
   *  FROM Products
   *  GROUP BY CategoryID;
   *
   *  SELECT DISTINCT ProductID, CategoryID
   *  From Products
   */

  /** @brief Aggregate columns */
  std::vector<ExpressionType> aggregate_types;
  std::vector<oid_t> aggregate_columns;

  /** @brief Group by columns */
  std::vector<oid_t> group_by_columns;

  /** @brief Pass through columns */
  std::vector<oid_t> pass_through_columns;

};

} // namespace planner
} // namespace nstore

