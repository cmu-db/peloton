//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity.h
//
// Identification: src/include/optimizer/stats/selectivity.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <vector>
#include <algorithm>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/stats_util.h"

namespace peloton {
namespace optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class Selectivity {
public:

  // Get expression selectivity.
  static double GetSelectivity(oid_t database_id, oid_t table_id, oid_t column_id,
    const type::Value& value, ExpressionType type) {
      switch (type) {
        case ExpressionType::COMPARE_LESSTHAN:
          return GetLessThanSelectivity(database_id, table_id, column_id, value);
        case ExpressionType::COMPARE_GREATERTHAN:
          return GetGreaterThanSelectivity();
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          return GetLessThanOrEqualToSelectivity();
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          return GetGreaterThanOrEqualToSelectivity();
        default:
          return -1; // TODO: return something more meaningful
      }
  }

  // Selectivity of '<' expression.
  static double GetLessThanSelectivity(oid_t database_id, oid_t table_id,
                                       oid_t column_id, const type::Value& value) {
    // Convert peloton value to double
    UNUSED_ATTRIBUTE double v = PelotonValueToNumericValue(value);

    // Get column stats and histogram from stats storage
    auto stats_storage = optimizer::StatsStorage::GetInstance();
    auto column_stats = stats_storage->GetColumnStatsByID(database_id, table_id, column_id);
    if (column_stats == nullptr) {
      return DEFAULT_SELECTIVITY;
    }
    std::vector<double> histogram = column_stats->histogram_bounds;
    size_t n = histogram.size();
    PL_ASSERT(n > 0);

    // find correspond bin using binary serach
    auto it = std::lower_bound(histogram.begin(), histogram.end(), v);
    return (it - histogram.begin()) * 1.0 / n;
  }

  // Selectivity of '<=' expression.
  static double GetLessThanOrEqualToSelectivity() {
    return 0;
  }

  // Selectivity of '>' expression.
  static double GetGreaterThanSelectivity() {
    return 0;
  }

  // Selectivity of '>=' expression.
  static double GetGreaterThanOrEqualToSelectivity() {
    return 0;
  }

  static double GetEqualSelectivity() {
    return 0;
  }

  static double GetNotEqualSelectivity() {
    return 0;
  }

  static double GetLikeSelectivity() {
    return 0;
  }

  static double GetNotLikeSelectivity() {
    return 0;
  }
};

} /* namespace optimizer */
} /* namespace peloton */
