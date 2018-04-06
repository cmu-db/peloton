//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity.cpp
//
// Identification: src/optimizer/stats/selectivity.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/selectivity.h"

#include "catalog/table_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "function/string_functions.h"

namespace peloton {
namespace optimizer {

double Selectivity::ComputeSelectivity(const std::shared_ptr<TableStats> &stats,
                                       const ValueCondition &condition) {
  switch (condition.type) {
    case ExpressionType::COMPARE_LESSTHAN:
      return LessThan(stats, condition);
    case ExpressionType::COMPARE_GREATERTHAN:
      return GreaterThan(stats, condition);
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return LessThanOrEqualTo(stats, condition);
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return GreaterThanOrEqualTo(stats, condition);
    case ExpressionType::COMPARE_EQUAL:
      return Equal(stats, condition);
    case ExpressionType::COMPARE_NOTEQUAL:
      return NotEqual(stats, condition);
    case ExpressionType::COMPARE_LIKE:
      return Like(stats, condition);
    case ExpressionType::COMPARE_NOTLIKE:
      return NotLike(stats, condition);
    case ExpressionType::COMPARE_IN:
      return In(stats, condition);
    case ExpressionType::COMPARE_DISTINCT_FROM:
      return DistinctFrom(stats, condition);
    default:
      LOG_WARN("Expression type %s not supported for computing selectivity",
               ExpressionTypeToString(condition.type).c_str());
      return DEFAULT_SELECTIVITY;
  }
}

double Selectivity::LessThan(const std::shared_ptr<TableStats> &table_stats,
                             const ValueCondition &condition) {
  // Convert peloton value type to raw value (double)
  double v = StatsUtil::PelotonValueToNumericValue(condition.value);
  if (std::isnan(v)) {
    LOG_TRACE("Error computing less than for non-numeric type");
    return DEFAULT_SELECTIVITY;
  }
  // TODO: make sure condition uses column id. check if column name is not
  // empty.
  std::shared_ptr<ColumnStats> column_stats =
      table_stats->GetColumnStats(condition.column_name);
  // Return default selectivity if no column stats for given column_id
  if (column_stats == nullptr) {
    return DEFAULT_SELECTIVITY;
  }
  // Use histogram to estimate selectivity
  std::vector<double> histogram = column_stats->histogram_bounds;
  size_t n = histogram.size();
  PELOTON_ASSERT(n > 0);
  // find correspond bin using binary search
  auto it = std::lower_bound(histogram.begin(), histogram.end(), v);
  double res = (it - histogram.begin()) * 1.0 / n;
  PELOTON_ASSERT(res >= 0);
  PELOTON_ASSERT(res <= 1);
  return res;
}

double Selectivity::Equal(const std::shared_ptr<TableStats> &table_stats,
                          const ValueCondition &condition) {
  double value = StatsUtil::PelotonValueToNumericValue(condition.value);
  auto column_stats = table_stats->GetColumnStats(condition.column_name);
  //  LOG_INFO("column name %s", condition.column_name);
  if (std::isnan(value) || column_stats == nullptr) {
    LOG_DEBUG("Calculate selectivity: return null");
    return DEFAULT_SELECTIVITY;
  }

  size_t numrows = column_stats->num_rows;
  // For now only double is supported in stats storage
  std::vector<double> most_common_vals = column_stats->most_common_vals;
  std::vector<double> most_common_freqs = column_stats->most_common_freqs;
  std::vector<double>::iterator first = most_common_vals.begin(),
                                last = most_common_vals.end();

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

    res = most_common_freqs[idx] / (double)numrows;
  } else {
    // the target value for equality comparison (parm value) is
    // NOT found in most common values
    // (1 - sum(mvf))/(num_distinct - num_mcv)
    double sum_mvf = 0;
    std::vector<double>::iterator first = most_common_freqs.begin(),
                                  last = most_common_freqs.end();
    while (first != last) {
      sum_mvf += *first;
      ++first;
    }

    if (numrows == 0 || column_stats->cardinality == most_common_vals.size()) {
      LOG_TRACE("Equal selectivity division by 0.");
      return DEFAULT_SELECTIVITY;
    }

    res = (1 - sum_mvf / (double)numrows) /
          (column_stats->cardinality - most_common_vals.size());
  }
  PELOTON_ASSERT(res >= 0);
  PELOTON_ASSERT(res <= 1);
  return res;
}

// Selectivity for 'LIKE' operator. The column type must be VARCHAR.
// Complete implementation once we support LIKE operator.
double Selectivity::Like(const std::shared_ptr<TableStats> &table_stats,
                         const ValueCondition &condition) {
  // Check whether column type is VARCHAR.
  if ((condition.value).GetTypeId() != type::TypeId::VARCHAR) {
    return DEFAULT_SELECTIVITY;
  }

  const char *pattern = (condition.value).GetData();

  auto column_stats = table_stats->GetColumnStats(condition.column_name);
  if (column_stats == nullptr) {
    return DEFAULT_SELECTIVITY;
  }
  oid_t column_id = column_stats->column_id;

  size_t matched_count = 0;
  size_t total_count = 0;

  // Sample on the fly
  auto sampler = table_stats->GetSampler();
  PELOTON_ASSERT(sampler != nullptr);
  if (sampler->GetSampledTuples().empty()) {
    sampler->AcquireSampleTuples(DEFAULT_SAMPLE_SIZE);
  }
  auto &sample_tuples = sampler->GetSampledTuples();
  for (size_t i = 0; i < sample_tuples.size(); i++) {
    auto value = sample_tuples[i]->GetValue(column_id);
    PELOTON_ASSERT(value.GetTypeId() == type::TypeId::VARCHAR);
    executor::ExecutorContext dummy_context(nullptr);
    if (function::StringFunctions::Like(dummy_context, value.GetData(),
                                        value.GetLength(), pattern,
                                        condition.value.GetLength())) {
      matched_count++;
    }
  }
  total_count = sample_tuples.size();
  LOG_TRACE("total sample size %lu matched tupe %lu", total_count,
            matched_count);

  return total_count == 0 ? DEFAULT_SELECTIVITY
                          : (double)matched_count / total_count;
}

}  // namespace optimizer
}  // namespace peloton
