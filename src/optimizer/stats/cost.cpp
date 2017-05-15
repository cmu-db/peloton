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
double Cost::SingleConditionSeqScanCost(
    const std::shared_ptr<TableStats> &input_stats,
    const ValueCondition &condition,
    std::shared_ptr<TableStats> &output_stats) {
  // PL_ASSERT(input_stats != nullptr);
  // PL_ASSERT(condition != nullptr);

  UpdateConditionStats(input_stats, condition, output_stats);

  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

double Cost::SingleConditionIndexScanCost(
    const std::shared_ptr<TableStats> &input_stats,
    const ValueCondition &condition,
    std::shared_ptr<TableStats> &output_stats) {
  double index_height = default_index_height(input_stats->num_rows);
  double index_cost = index_height * DEFAULT_INDEX_TUPLE_COST;

  double selectivity = Selectivity::ComputeSelectivity(input_stats, condition);
  double scan_cost = selectivity * DEFAULT_TUPLE_COST;

  UpdateConditionStats(input_stats, condition, output_stats);

  return index_cost + scan_cost;
}

void Cost::CombineConjunctionStats(const std::shared_ptr<TableStats> &lhs,
                                   const std::shared_ptr<TableStats> &rhs,
                                   const size_t num_rows,
                                   const ExpressionType type,
                                   std::shared_ptr<TableStats> &output_stats) {
  PL_ASSERT(lhs != nullptr);
  PL_ASSERT(rhs != nullptr);
  PL_ASSERT(num_rows > 0);

  size_t num_tuples = 1;
  double sel1 = lhs->num_rows / static_cast<double>(num_rows);
  double sel2 = rhs->num_rows / static_cast<double>(num_rows);
  LOG_TRACE("Conjunction sel1[%f] sel2[%f]", sel1, sel2);
  switch (type) {
    case ExpressionType::CONJUNCTION_AND:
      // (sel1 * sel2) * num_rows
      num_tuples = static_cast<size_t>(num_rows * sel1 * sel2);
      break;
    case ExpressionType::CONJUNCTION_OR:
      // (sel1 + sel2 - sel1 * sel2) * num_rows
      num_tuples = static_cast<size_t>((sel1 + sel2 - sel1 * sel2) * num_rows);
      break;
    default:
      LOG_TRACE("Cost model conjunction on expression type %d not supported",
                type);
  }
  if (output_stats != nullptr) {
    output_stats->num_rows = num_tuples;
  }
}

//===----------------------------------------------------------------------===//
// GROUP BY
//===----------------------------------------------------------------------===//
double Cost::SortGroupByCost(const std::shared_ptr<TableStats> &input_stats,
                             std::vector<oid_t> columns,
                             std::shared_ptr<TableStats> &output_stats) {
  PL_ASSERT(input_stats);
  PL_ASSERT(columns.size() > 0);

  if (output_stats != nullptr) {
    output_stats->num_rows = GetEstimatedGroupByRows(input_stats, columns);
  }

  double cost =
      default_sorting_cost(input_stats->num_rows) * DEFAULT_TUPLE_COST;

  // Update cost to trivial if first group by column has index.
  // TODO: use more complicated cost when group by multiple columns when
  // primary index operator is supported.
  if (input_stats->HasPrimaryIndex(columns[0])) {
    // underestimation of group by with index.
    cost = DEFAULT_OPERATOR_COST;
  }

  return cost;
}

double Cost::HashGroupByCost(const std::shared_ptr<TableStats> &input_stats,
                             std::vector<oid_t> columns,
                             std::shared_ptr<TableStats> &output_stats) {
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
double Cost::DistinctCost(const std::shared_ptr<TableStats> &input_stats,
                          oid_t column,
                          std::shared_ptr<TableStats> &output_stats) {
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
double ProjectCost(const std::shared_ptr<TableStats> &input_stats,
                   UNUSED_ATTRIBUTE std::vector<oid_t> columns,
                   std::shared_ptr<TableStats> &output_stats) {
  PL_ASSERT(input_stats);

  if (output_stats != nullptr) {
    // update column information for output_stats table
  }

  return input_stats->num_rows * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// LIMIT
//===----------------------------------------------------------------------===//
double Cost::LimitCost(const std::shared_ptr<TableStats> &input_stats,
                       size_t limit,
                       std::shared_ptr<TableStats> &output_stats) {
  PL_ASSERT(input_stats != nullptr);
  if (output_stats != nullptr) {
    output_stats->num_rows = std::max(input_stats->num_rows, limit);
  }
  return limit * DEFAULT_TUPLE_COST;
}

//===----------------------------------------------------------------------===//
// ORDER BY
//===----------------------------------------------------------------------===//
double OrderByCost(const std::shared_ptr<TableStats> &input_stats,
                   const std::vector<oid_t> &columns,
                   const std::vector<bool> &orders,
                   std::shared_ptr<TableStats> &output_stats) {
  PL_ASSERT(input_stats);
  // Invalid case.
  if (columns.size() == 0 || columns.size() != orders.size()) {
    return DEFAULT_COST;
  }
  oid_t column = columns[0];
  bool order = orders[0];  // TRUE is ASC, FALSE is DESC
  double cost = DEFAULT_COST;
  // Special case when first column has index.
  if (input_stats->HasPrimaryIndex(column)) {
    if (order) {  // ascending
      // No cost for order by for now. We might need to take
      // cardinality of first column into account in the future.
      cost = DEFAULT_OPERATOR_COST;
    } else {  // descending
      // Reverse sequence.
      cost = input_stats->num_rows * DEFAULT_TUPLE_COST;
    }
  } else {
    cost = default_sorting_cost(input_stats->num_rows) * DEFAULT_TUPLE_COST;
  }
  if (output_stats != nullptr) {
    output_stats->num_rows = input_stats->num_rows;
    // Also set HasPrimaryIndex for first column to true.
  }
  return cost;
}

//===----------------------------------------------------------------------===//
// Helper functions
//===----------------------------------------------------------------------===//
void Cost::UpdateConditionStats(const std::shared_ptr<TableStats> &input_stats,
                                const ValueCondition &condition,
                                std::shared_ptr<TableStats> &output_stats) {
  if (output_stats != nullptr) {
    double selectivity =
        Selectivity::ComputeSelectivity(input_stats, condition);
    output_stats->num_rows = input_stats->num_rows * selectivity;
  }
}

size_t Cost::GetEstimatedGroupByRows(
    const std::shared_ptr<TableStats> &input_stats,
    std::vector<oid_t> &columns) {
  // Idea is to assume each column is uniformaly distributed and get an
  // overestimation.
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
