//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost.h
//
// Identification: src/include/optimizer/stats/cost.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <cmath>
#include <memory>

#include "table_stats.h"
#include "value_condition.h"
#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace optimizer {

// Default cost when cost model cannot compute correct cost.
static constexpr double DEFAULT_COST = 1;

// Estimate the cost of processing each row during a query.
static constexpr double DEFAULT_TUPLE_COST = 0.01;

// Estimate the cost of processing each index entry during an index scan.
static constexpr double DEFAULT_INDEX_TUPLE_COST = 0.005;

// Estimate the cost of processing each operator or function executed during a
// query.
static constexpr double DEFAULT_OPERATOR_COST = 0.0025;

// Default cost of sorting n elements
  double default_sorting_cost(size_t n) { return n * std::log2(n); }

// Default number of index tuple to access for n elements
  double default_index_height(size_t n) { return std::log2(n); }

//===----------------------------------------------------------------------===//
// Cost
//===----------------------------------------------------------------------===//
class Cost {
 public:
  /*
   * Cost of scan for single condition. For scan with multiple conditions,
   * you should use CombineConjunctionStats to combine output_stats.
   */
  static double SingleConditionSeqScanCost(
      const std::shared_ptr<TableStats>& input_stats,
      const ValueCondition& condition,
      std::shared_ptr<TableStats>& output_stats);

  static double SingleConditionIndexScanCost(
      const std::shared_ptr<TableStats>& input_stats,
      const ValueCondition& condition,
      std::shared_ptr<TableStats>& output_stats);

  /*
   *  Combine two stats with conjunction clause.
   *  ExpressionType type can be CONJUNCTION_AND / CONJUNCTION_OR
   */
  static void CombineConjunctionStats(
      const std::shared_ptr<TableStats>& lhs,
      const std::shared_ptr<TableStats>& rhs, const size_t num_rows,
      const ExpressionType type, std::shared_ptr<TableStats>& output_stats);

  /*
   * Cost of GROUP BY.
   */
  static double SortGroupByCost(const std::shared_ptr<TableStats>& input_stats,
                                std::vector<oid_t> columns,
                                std::shared_ptr<TableStats>& output_stats);

  static double HashGroupByCost(const std::shared_ptr<TableStats>& input_stats,
                                std::vector<oid_t> columns,
                                std::shared_ptr<TableStats>& output_stats);

  /*
   * Aggregation (SUM, COUNT, etc) cost = cost of scan input table.
   * Note it does not update table stats.
   */
  static inline double AggregateCost(
      const std::shared_ptr<TableStats>& input_stats) {
    PL_ASSERT(input_stats != nullptr);
    return input_stats->num_rows * DEFAULT_TUPLE_COST;
  }

  /*
   * Cost of DISTINCT = cost of building hash table.
   */
  static double DistinctCost(const std::shared_ptr<TableStats>& input_stats,
                             oid_t column,
                             std::shared_ptr<TableStats>& output_stats);

  /*
   * Cost of projection = full table scan.
   */
  static double ProjectCost(const std::shared_ptr<TableStats>& input_stats,
                            std::vector<oid_t> columns,
                            std::shared_ptr<TableStats>& output_stats);

  /*
   * Cost of LIMIT = limit * tuple_cost
   */
  static double LimitCost(const std::shared_ptr<TableStats>& input_stats,
                          size_t limit,
                          std::shared_ptr<TableStats>& output_stats);

  /*
   * Cost of ORDER BY = cost of sorting or 1 if column has index.
   * Note right only first column is taken into consideration.
   */
  static double OrderByCost(const std::shared_ptr<TableStats>& input_stats,
                            const std::vector<oid_t>& columns,
                            const std::vector<bool>& orders,
                            std::shared_ptr<TableStats>& output_stats);

  /*
   * Join
   */
  inline static double InnerNLJoin() { return DEFAULT_COST; }

  inline static double InnerHashJoin() { return DEFAULT_COST; }

  /*
   * Update output statistics given input table and one condition.
   * Updated stats will be placed in output_stats.
   */
  static void UpdateConditionStats(
      const std::shared_ptr<TableStats>& input_stats,
      const ValueCondition& condition,
      std::shared_ptr<TableStats>& output_stats);

  /*
   * Return estimated number of rows after group by operation.
   * This function is used by HashGroupBy and SortGroupBy.
   */
  static size_t GetEstimatedGroupByRows(
      const std::shared_ptr<TableStats>& input_stats,
      std::vector<oid_t>& columns);
};

} /* namespace optimizer */
} /* namespace peloton */
