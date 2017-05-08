//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost.cpp
//
// Identification: src/optimizer/stats/cost.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/value.h"
#include "optimizer/stats/selectivity.h"
#include "expression/comparison_expression.h"
#include "optimizer/stats/cost.h"

#include <cmath>

namespace peloton {
namespace optimizer {

//===----------------------------------------------------------------------===//
// Scan
//===----------------------------------------------------------------------===//
double Cost::SingleConditionSeqScanCost(TableStats* input_stats,
  ValueCondition* condition, TableStats* output_stats) {

  PL_ASSERT(input_stats != nullptr);
  PL_ASSERT(condition != nullptr);

  UpdateConditionStats(input_stats, condition, output_stats);

  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

double Cost::SingleConditionIndexScanCost(TableStats* input_stats,
  ValueCondition* condition, TableStats* output_stats) {

  double index_height = default_index_height(input_stats->num_rows);
  double index_cost = index_height * DEFAULT_INDEX_TUPLE_COST;

  double selectivity = Selectivity::ComputeSelectivity(input_stats, condition);
  double scan_cost = selectivity * DEFAULT_TUPLE_COST;

  UpdateConditionStats(input_stats, condition, output_stats);

  return index_cost + scan_cost;
}

void CombineConjunctionStats(TableStats* lhs, TableStats* rhs, ExpressionType type,
  TableStats* output_stats) {
  PL_ASSERT(lhs != nullptr);
  PL_ASSERT(rhs != nullptr);

  size_t num_tuples = 1;
  switch (type) {
    case ExpressionType::CONJUNCTION_AND:
      // Minimum of two stats rows, overestimation.
      num_tuples = std::min(lhs->num_rows, rhs->num_rows);
      break;
    case ExpressionType::CONJUNCTION_OR:
      // Maximum of two stats rows, underestimation.
      num_tuples = std::max(lhs->num_rows, rhs->num_rows);
      break;
    default:
      LOG_TRACE("Cost model conjunction on expression type %d not supported", type);
  }
  if (output_stats != nullptr) {
    output_stats->num_rows = num_tuples;
  }
}

//===----------------------------------------------------------------------===//
// GROUP BY
//===----------------------------------------------------------------------===//
double Cost::SortGroupByCost(TableStats* input_stats, std::vector<oid_t> columns, TableStats* output_stats) {
  PL_ASSERT(input_stats);
  PL_ASSERT(columns.size() > 0);

  if (output_stats != nullptr) {
    output_stats->num_rows = GetEstimatedGroupByRows(input_stats, columns);
  }

  double cost = default_sorting_cost(input_stats->num_rows) * DEFAULT_TUPLE_COST;

  // Update cost to trivial if first group by column has index.
  // TODO: use more complicated cost when group by multiple columns when
  // primary index operator is supported.
  if (input_stats->HasPrimaryIndex(columns[0])) {
    // underestimation of group by with index.
    cost = DEFAULT_OPERATOR_COST;
  }

  return cost;
}

double Cost::HashGroupByCost(TableStats* input_stats,
  std::vector<oid_t> columns, TableStats* output_stats) {

  PL_ASSERT(input_stats);

  if (output_stats != nullptr) {
    output_stats->num_rows = GetEstimatedGroupByRows(input_stats, columns);
  }

  // Directly hash tuple
  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// DISTINCT
//===----------------------------------------------------------------------===//
double Cost::DistinctCost(TableStats* input_stats, oid_t column, TableStats* output_stats) {
  PL_ASSERT(input_stats);

  if (output_stats != nullptr) {
    // update number of rows to be number of unique element of column
    output_stats->num_rows = input_stats->GetCardinality(column);
  }
  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// Project
//===----------------------------------------------------------------------===//
double ProjectCost(TableStats* input_stats, UNUSED_ATTRIBUTE std::vector<oid_t> columns,
  TableStats* output_stats) {
  PL_ASSERT(input_stats);

  if (output_stats != nullptr) {
    // update column information for output_stats table
  }

  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// LIMIT
//===----------------------------------------------------------------------===//
double Cost::LimitCost(TableStats* input_stats, size_t limit, TableStats* output_stats) {
  PL_ASSERT(input_stats != nullptr);
  if (output_stats != nullptr) {
    output_stats->num_rows = std::max(input_stats->num_rows, limit);
  }
  return limit * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// ORDER BY
//===----------------------------------------------------------------------===//


//===----------------------------------------------------------------------===//
// Helper functions
//===----------------------------------------------------------------------===//
void Cost::UpdateConditionStats(TableStats* input_stats, ValueCondition* condition,
  TableStats* output_stats) {
  if (output_stats != nullptr) {
    double selectivity = Selectivity::ComputeSelectivity(input_stats, condition);
    output_stats->num_rows = input_stats->num_rows * selectivity;
  }
}

size_t Cost::GetEstimatedGroupByRows(TableStats* input_stats, std::vector<oid_t> columns) {
  // Idea is to assume each column is uniformaly distributed and get an overestimation.
  // Then use max cardinality among all columns as underestimation.
  // And combine them together.
  double rows = 1;
  double max_cardinality = 0;
  for (oid_t column : columns) {
    double cardinality = input_stats->GetCardinality(column);
    max_cardinality = std::max(max_cardinality, cardinality);
    rows *= cardinality;
  }
  return static_cast<size_t>(rows + max_cardinality / 2);
}

} /* namespace optimizer */
} /* namespace peloton */
