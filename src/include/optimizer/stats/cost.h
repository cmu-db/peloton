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

#include "table_stats.h"
#include "value_condition.h"
#include "common/logger.h"
#include "common/macros.h"

namespace peloton{
namespace optimizer{

// Default cost when cost model cannot compute correct cost.
static constexpr double DEFAULT_COST = 1;

// Estimate the cost of processing each row during a query.
static constexpr double DEFAULT_TUPLE_COST = 0.01;

// Estimate the cost of processing each index entry during an index scan.
static constexpr double DEFAULT_INDEX_TUPLE_COST = 0.005;

// Estimate the cost of processing each operator or function executed during a query.
static constexpr double DEFAULT_OPERATOR_COST = 0.0025;

// Default cost of sorting n elements
constexpr double default_sorting_cost(size_t n) {
  return n * std::log2(n);
}

// Default number of index tuple to access for n elements
constexpr double default_index_height(size_t n) {
  return std::log2(n);
}

//===----------------------------------------------------------------------===//
// Cost
//===----------------------------------------------------------------------===//
class Cost {
public:

  /*
   * Cost of scan for single condition. You should use xxx to merge
   * stats of conjunctive conditions.
   */
  static double SingleConditionSeqScanCost(TableStats* input_stats,
    ValueCondition* condition, TableStats* output_stats = nullptr);

  static double SingleConditionIndexScanCost(TableStats* input_stats,
    ValueCondition* condition, TableStats* output_stats = nullptr);

  /*
   *  Combine two stats with conjunction clause.
   *  ExpressionType type can be CONJUNCTION_AND / CONJUNCTION_OR
   */
  static void CombineConjunctionStats(TableStats* lhs, TableStats* rhs,
    ExpressionType type, TableStats* output_stats = nullptr);

  /*
   * Cost of GROUP BY.
   */
  static double SortGroupByCost(TableStats* input_stats,
    std::vector<oid_t> columns, TableStats* output_stats = nullptr);

  static double HashGroupByCost(TableStats* input_stats,
    std::vector<oid_t> columns, TableStats* output_stats = nullptr);

  /*
   * Aggregation (SUM, COUNT, etc) cost = cost of scan input table.
   * Note it does not update table stats.
   */
  static inline double AggregateCost(TableStats* input_stats) {
    PL_ASSERT(input_stats != nullptr);
    return input_stats->num_rows * DEFAULT_TUPLE_COST;
  }

  /*
   * Cost of DISTINCT = cost of building hash table.
   */
  static double DistinctCost(TableStats* input_stats, oid_t column,
    TableStats* output_stats = nullptr);

  /*
   * Cost of projection = full table scan.
   */
  static double ProjectCost(TableStats* input_stats, std::vector<oid_t> columns,
    TableStats* output_stats = nullptr);

  /*
   * Cost of LIMIT = limit * tuple_cost
   */
  static double LimitCost(TableStats* input_stats, size_t limit,
    TableStats* output_stats = nullptr);

  /*
   * Cost of ORDER BY.
   * TODO: implement me!
   */
  static double OrderByCost(TableStats* input_stats, std::vector<oid_t> columns,
    std::vector<bool> orders, TableStats* output_stats = nullptr);

  /*
   * Join
   */
  inline static double InnerNLJoin() {
    return DEFAULT_COST;
  }

  inline static double InnerHashJoin() {
    return DEFAULT_COST;
  }

  /*
   * Update output statistics given input table and one condition.
   * Updated stats will be placed in output_stats.
   */
  static void UpdateConditionStats(TableStats* input_stats, ValueCondition* condition,
    TableStats* output_stats);

  /*
   * Return estimated number of rows after group by operation.
   * This function is used by HashGroupBy and SortGroupBy.
   */
  static size_t GetEstimatedGroupByRows(TableStats* input_stats, std::vector<oid_t> columns);
};

} /* namespace optimizer */
} /* namespace peloton */
