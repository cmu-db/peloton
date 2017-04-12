#pragma once

#include <vector>
#include <functional>
#include <libcount/hll.h>

#include "type/types.h"
#include "optimizer/stats/count_min_sketch.h"
#include "optimizer/stats/top_k_elements.h"
#include "optimizer/stats/histogram.h"
#include "optimizer/stats/hll.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStats
//===--------------------------------------------------------------------===//
class ColumnStats {
public:
  /* Default parameters for probabilistic stats collector */
  int hll_precision = 8;
  double cmsketch_eps = 0.1;
  double cmsketch_gamma = 0.01;
  uint8_t max_bins = 64;
  uint8_t num_bins = 10;
  uint8_t top_k = 10;

  using ValueFrequencyPair = std::pair<type::Value, double>;

  ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
              type::Type::TypeId column_type);

  ~ColumnStats();

  void AddValue(type::Value& value);

  double GetFracNull();

  std::vector<ValueFrequencyPair> GetCommonValueAndFrequency();

  uint64_t GetCardinality();

  std::vector<double> GetHistogramBound();

 private:
  const oid_t database_id_;
  const oid_t table_id_;
  const oid_t column_id_;
  const type::Type::TypeId column_type_;
  HLL hll_;
  Histogram hist_;
  CountMinSketch sketch_;
  TopKElements topk_;

  std::function<void(type::Value&)> f_add_value;

  // Not allow copy
  ColumnStats(const ColumnStats&);
  void operator=(const ColumnStats&);

  size_t null_count_ = 0;   // Number of null values
  size_t total_count_ = 0;  // Total number of values

  void CheckColumnType(type::Type::TypeId type);
  uint8_t TuneHLLPrecision(type::Type::TypeId type);
  void ComputeTrivialStats(type::Value& value);
  void ComputeScalarStats(type::Value& value);
  void ComputeDistinctStats(type::Value& value);
};

} /* namespace optimizer */
} /* namespace peloton */
