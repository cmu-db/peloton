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
#include <limits>
#include <vector>
#include <algorithm>
#include <memory>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "catalog/column_catalog.h"
#include "catalog/catalog.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/value_condition.h"
#include "optimizer/stats/column_stats.h"

namespace peloton {
namespace optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class Selectivity {
 public:
  static double ComputeSelectivity(
      const std::shared_ptr<TableStats>& table_stats,
      const ValueCondition& condition);

  static double LessThan(const std::shared_ptr<TableStats>& table_stats,
                         const ValueCondition& condition);

  static double LessThanOrEqualTo(
      const std::shared_ptr<TableStats>& table_stats,
      const ValueCondition& condition) {
    double res =
        LessThan(table_stats, condition) + Equal(table_stats, condition);
    return std::max(std::min(res, 1.0), 0.0);
  }

  static double GreaterThan(const std::shared_ptr<TableStats>& table_stats,
                            const ValueCondition& condition) {
    return 1 - LessThanOrEqualTo(table_stats, condition);
  }

  static double GreaterThanOrEqualTo(
      const std::shared_ptr<TableStats>& table_stats,
      const ValueCondition& condition) {
    return 1 - LessThan(table_stats, condition);
  }

  static double Equal(const std::shared_ptr<TableStats>& table_stats,
                      const ValueCondition& condition);

  static double NotEqual(const std::shared_ptr<TableStats>& table_stats,
                         const ValueCondition& condition) {
    return 1 - Equal(table_stats, condition);
  }

  // Selectivity for 'LIKE' operator. The column type must be VARCHAR.
  // Complete implementation once we support LIKE operator.
  static double Like(const std::shared_ptr<TableStats>& table_stats,
                     const ValueCondition& condition);

  static double NotLike(const std::shared_ptr<TableStats>& table_stats,
                        const ValueCondition& condition) {
    return 1 - Like(table_stats, condition);
  }

  static double In(
      UNUSED_ATTRIBUTE const std::shared_ptr<TableStats>& table_stats,
      UNUSED_ATTRIBUTE const ValueCondition& condition) {
    return DEFAULT_SELECTIVITY;
  }

  static double DistinctFrom(
      UNUSED_ATTRIBUTE const std::shared_ptr<TableStats> table_stats,
      UNUSED_ATTRIBUTE const ValueCondition& condition) {
    return DEFAULT_SELECTIVITY;
  }
};

} /* namespace optimizer */
} /* namespace peloton */
