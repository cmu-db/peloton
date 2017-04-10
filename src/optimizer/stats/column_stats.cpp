#include "optimizer/stats/column_stats.h"

#include "common/macros.h"

namespace peloton {
namespace optimizer {

ColumnStats::ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
  type::Type::TypeId column_type)
    : database_id_{database_id},
      table_id_{table_id},
      column_id_{column_id},
      column_type_{column_type},
      hll_{},
      hist_{},
      sketch_{100, 100, 0},
      topk_{sketch_, 10} {}

ColumnStats::~ColumnStats() {}

void ColumnStats::AddValue(type::Value& value) {
  hll_.Update(value);
  hist_.Update(value);
  topk_.Add(value);
  total_count_++;
  if (value.IsNull()) {
    null_count_++;
  }
}

double ColumnStats::GetFracNull() {
  if (total_count_ == 0) {
    PL_ASSERT("Cannot calculate stats for table size 0.");
    return 0;
  }
  return (static_cast<double>(null_count_) / total_count_);
}

std::vector<ColumnStats::ValueFrequencyPair> ColumnStats::GetCommonValueAndFrequency() {
  return topk_.GetAllOrderedMaxFirst();
}

uint64_t ColumnStats::GetCardinality() {
  return hll_.EstimateCardinality();
}

std::vector<double> ColumnStats::GetHistogramBound() {
  return hist_.Uniform(num_bins);
}

} /* namespace optimizer */
} /* namespace peloton */
