//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats.cpp
//
// Identification: src/optimizer/stats/column_stats.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
      hll_{TuneHLLPrecision(column_type_)},
      hist_{max_bins},
      sketch_{cmsketch_eps, cmsketch_gamma, 0},
      topk_{sketch_, top_k} {
  CheckColumnType(column_type_);
}

ColumnStats::~ColumnStats() {}

void ColumnStats::CheckColumnType(type::Type::TypeId type) {
  switch (type) {
    case type::Type::PARAMETER_OFFSET:
    case type::Type::TINYINT:
    case type::Type::SMALLINT:
    case type::Type::INTEGER:
    case type::Type::BIGINT:
    case type::Type::DECIMAL:
    case type::Type::TIMESTAMP:
      f_add_value = [&](type::Value& v) { ComputeScalarStats(v); };
      break;
    case type::Type::BOOLEAN:
    case type::Type::DATE:
    case type::Type::VARCHAR:
    case type::Type::VARBINARY:
      f_add_value = [&](type::Value& v) { ComputeDistinctStats(v); };
      break;
    default:
      f_add_value = [&](type::Value& v) { ComputeTrivialStats(v); };
      break;
  }
}

uint8_t ColumnStats::TuneHLLPrecision(type::Type::TypeId type) {
  switch (type) {
    case type::Type::BIGINT:
    case type::Type::TIMESTAMP:
      return 5;  // TODO: use more complicated tuning
    default:
      return hll_precision;
  }
}

void ColumnStats::AddValue(type::Value& value) {
  if (value.GetTypeId() != column_type_) return;
  total_count_++;
  if (value.IsNull()) {
    null_count_++;
  }
  f_add_value(value);
}

/*
 * Value cannot be compared using >, < and =
 */
void ColumnStats::ComputeTrivialStats(UNUSED_ATTRIBUTE type::Value& value) {}

/*
 * Value can be compared using >, < and =
 */
void ColumnStats::ComputeScalarStats(type::Value& value) {
  hll_.Update(value);
  hist_.Update(value);
  topk_.Add(value);
}

/*
 * Value cannot be compared using > or <, but can be compared using =
 */
void ColumnStats::ComputeDistinctStats(type::Value& value) {
  hll_.Update(value);
  topk_.Add(value);
}

double ColumnStats::GetFracNull() {
  if (total_count_ == 0) {
    LOG_ERROR("Cannot calculate stats for table size 0.");
    return 0;
  }
  return (static_cast<double>(null_count_) / total_count_);
}

std::vector<ColumnStats::ValueFrequencyPair>
ColumnStats::GetCommonValueAndFrequency() {
  return topk_.GetAllOrderedMaxFirst();
}

uint64_t ColumnStats::GetCardinality() { return hll_.EstimateCardinality(); }

std::vector<double> ColumnStats::GetHistogramBound() {
  return hist_.Uniform(num_bins);
}

} /* namespace optimizer */
} /* namespace peloton */
