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
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "catalog/column_catalog.h"
#include "catalog/catalog.h"
#include "optimizer/stats/sel_param.h"

namespace peloton {
namespace optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class Selectivity {
public:

  // Get comparison operators selectivity.
  static double GetExpressionSelectivity(SelParam *param, ExpressionType type) {
      switch (type) {
        case ExpressionType::COMPARE_LESSTHAN:
          return LessThan(param);
        case ExpressionType::COMPARE_GREATERTHAN:
          return GreaterThan(param);
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          return LessThanOrEqualTo(param);
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          return GreaterThanOrEqualTo(param);
        case ExpressionType::COMPARE_EQUAL:
          return Equal(param);
        case ExpressionType::COMPARE_NOTEQUAL:
          return NotEqual(param);
        case ExpressionType::COMPARE_LIKE:
          return Like(param);
        case ExpressionType::COMPARE_NOTLIKE:
          return NotLike(param);
        case ExpressionType::COMPARE_IN:
          return In(param);
        case ExpressionType::COMPARE_DISTINCT_FROM:
          return DistinctFrom(param);
        default:
          LOG_TRACE("Expression type %d not supported for computing selectivity", type);
          return DEFAULT_SELECTIVITY;
      }
  }

  // Selectivity for '<' operator.
  static double LessThan(SelParam* param) {
    // Convert peloton value to double
    double v = PelotonValueToNumericValue(param->value);
    // Get column stats and histogram from stats storage
    auto stats_storage = optimizer::StatsStorage::GetInstance();
    auto column_stats = stats_storage->GetColumnStatsByID(
      param->database_id, param->table_id, param->column_id);
    // Return default selectivity if no column stats for given column_id
    if (column_stats == nullptr) {
      return DEFAULT_SELECTIVITY;
    }
    // Use histogram to estimate selectivity
    std::vector<double> histogram = column_stats->histogram_bounds;
    size_t n = histogram.size();
    PL_ASSERT(n > 0);
    // find correspond bin using binary serach
    auto it = std::lower_bound(histogram.begin(), histogram.end(), v);
    double res = (it - histogram.begin()) * 1.0 / n;
    PL_ASSERT(res >= 0);
    PL_ASSERT(res <= 1);
    return res;
  }

  // Selectivity for '<=' operator.
  static double LessThanOrEqualTo(SelParam *param) {
    double res = LessThan(param) + Equal(param);
    return std::max(res, 1.0);
  }

  // Selectivity for '>' operator.
  static double GreaterThan(SelParam *param) {
    return 1 - LessThanOrEqualTo(param);
  }

  // Selectivity for '>=' operator.
  static double GreaterThanOrEqualTo(SelParam *param) {
    return 1 - LessThan(param);
  }

  // Selectivity for '=' operator.
  static double Equal(SelParam* param) {
      auto stats_storage = optimizer::StatsStorage::GetInstance();
      auto column_stats = stats_storage->GetColumnStatsByID(
       param->database_id, param->table_id, param->column_id);
      double value = PelotonValueToNumericValue(param->value);

      // Stats not found.
      if (column_stats == nullptr) {
        return DEFAULT_SELECTIVITY;
      }

      size_t numrows = column_stats->num_row;
     // For now only double is supported in stats storage
     std::vector<double> most_common_vals = column_stats->most_common_vals;
     std::vector<double> most_common_freqs = column_stats->most_common_freqs;
     std::vector<double>::iterator first = most_common_vals.begin(), last = most_common_vals.end();

     while (first != last) {
       // For now only double is supported in stats storage
       if (*first == value) {
         break;
       }
       ++first;
     }

     double res = DEFAULT_SELECTIVITY;
     if (first != last) {
       // the target value for equality comparison (param value) is
       // found in most common values
       size_t idx = first - most_common_vals.begin();

       res = most_common_freqs[idx] / (double) numrows;
     } else {
       // the target value for equality comparison (parm value) is
       // NOT found in most common values
       // (1 - sum(mvf))/(num_distinct - num_mcv)
       double sum_mvf = 0;
       std::vector<double>::iterator first = most_common_freqs.begin(), last = most_common_freqs.end();
       while (first != last) {
         sum_mvf += *first;
         ++first;
       }

       res = (1 - sum_mvf / (double) numrows) / (column_stats->cardinality - most_common_vals.size());
    }
    PL_ASSERT(res >= 0);
    PL_ASSERT(res <= 1);
    return res;
  }

  // Selectivity for '!=' operator.
  static double NotEqual(SelParam* param) {
    return 1 - Equal(param);
  }

  // Selectivity for 'LIKE' operator. The column type must be VARCHAR.
  // Complete implementation once we support LIKE operator.
  static double Like(SelParam* param) {
    oid_t database_id = param->database_id;
    oid_t table_id = param->table_id;
    oid_t column_id = param->column_id;

    if ((param->value).GetTypeId() != type::Type::TypeId::VARCHAR) {
      return DEFAULT_SELECTIVITY;
    }
    UNUSED_ATTRIBUTE const char* pattern = (param->value).GetData();

    // Check whether column type is VARCHAR.
    auto column_catalog = catalog::ColumnCatalog::GetInstance();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    type::Type::TypeId column_type = column_catalog->GetColumnType(table_id, column_id, txn);
    txn_manager.CommitTransaction(txn);

    if(column_type != type::Type::TypeId::VARCHAR) {
      return DEFAULT_SELECTIVITY;
    }

    std::vector<type::Value> column_samples;
    auto tuple_storage = optimizer::TupleSamplesStorage::GetInstance();
    txn = txn_manager.BeginTransaction();
    tuple_storage->GetColumnSamples(database_id, table_id, column_id, column_samples);
    txn_manager.CommitTransaction(txn);

    for (size_t i = 0; i < column_samples.size(); i++) {
      LOG_DEBUG("Value: %s", column_samples[i].GetInfo().c_str());
    }

    return DEFAULT_SELECTIVITY;
  }

  // Selectivity for 'NOT LIKE' operator.
  static double NotLike(SelParam* param) {
    return 1 - Like(param);
  }

  // Selectivity for 'IN' operator.
  static double In(UNUSED_ATTRIBUTE SelParam* param) {
    return DEFAULT_SELECTIVITY;
  }

  // Selectivity for 'DISTINCT FROM' operator
  static double DistinctFrom(UNUSED_ATTRIBUTE SelParam* param) {
    return DEFAULT_SELECTIVITY;
  }
};

} /* namespace optimizer */
} /* namespace peloton */
