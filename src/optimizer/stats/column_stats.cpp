#include "optimizer/stats/column_stats.h"

#include <vector>

#include "optimizer/stats/hyperloglog.h"

namespace peloton {
namespace optimizer {

ColumnStats::ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
  type::Type::TypeId column_type)
    : database_id_{database_id},
      table_id_{table_id},
      column_id_{column_id},
      column_type_{column_type} {
  InitStatsCollectors();
}

ColumnStats::~ColumnStats() {}

/*
 * Support the following stats collectors:
 * - DistinctValueCounter
 */
void ColumnStats::InitStatsCollectors() {
  /* Init distinct value counter */
  HyperLogLog hll{};
  distinct_value_counter_ = &hll;

  /* Init value frequency counter */

  /* Init histogram builder */

  /* Init top k value counter */
}

void ColumnStats::AddValue(type::Value value) {
  distinct_value_counter_->AddValue(value);
}

double ColumnStats::GetNullValueCount() { return 0; }

std::vector<ColumnStats::ValueFrequencyPair> ColumnStats::GetCommonValueAndFrequency() {
  std::vector<ColumnStats::ValueFrequencyPair> res{};
  return res;
}

double ColumnStats::GetCardinality() {
  return distinct_value_counter_->EstimateCardinality();
}

std::vector<type::Value> ColumnStats::GetHistogramBound(uint8_t bin_nums = 10) {
  std::vector<type::Value> res(bin_nums);
  return res;
}

} /* namespace optimizer */
} /* namespace peloton */
