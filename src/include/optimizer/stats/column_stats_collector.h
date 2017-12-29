//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats_collector.h
//
// Identification: src/include/optimizer/stats/column_stats_collector.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <functional>
#include <libcount/hll.h>

#include "common/internal_types.h"
#include "optimizer/stats/count_min_sketch.h"
#include "optimizer/stats/top_k_elements.h"
#include "optimizer/stats/histogram.h"
#include "optimizer/stats/hyperloglog.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStatsCollector
//===--------------------------------------------------------------------===//
class ColumnStatsCollector {
 public:
  /* Default parameters for probabilistic stats collector */
  int hll_precision = 8;
  double cmsketch_eps = 0.01;
  double cmsketch_gamma = 0.01;
  uint8_t max_bins = 100;
  uint8_t top_k = 10;

  using ValueFrequencyPair = std::pair<type::Value, double>;

  ColumnStatsCollector(oid_t database_id, oid_t table_id, oid_t column_id,
                       type::TypeId column_type, std::string column_name);

  ~ColumnStatsCollector();

  void AddValue(const type::Value& value);

  double GetFracNull();

  inline std::vector<ValueFrequencyPair> GetCommonValueAndFrequency() {
    return topk_.GetAllOrderedMaxFirst();
  }

  inline uint64_t GetCardinality() { return hll_.EstimateCardinality(); }

  inline double GetCardinalityError() { return hll_.RelativeError(); }

  inline std::vector<double> GetHistogramBound() { return hist_.Uniform(); }

  inline std::string GetColumnName() { return column_name_; }

  inline void SetColumnIndexed() { has_index_ = true; }

  inline bool HasIndex() { return has_index_; }

 private:
  const oid_t database_id_;
  const oid_t table_id_;
  const oid_t column_id_;
  const type::TypeId column_type_;
  const std::string column_name_;
  HyperLogLog hll_;
  Histogram hist_;
  CountMinSketch sketch_;
  TopKElements topk_;

  bool has_index_ = false;

  size_t null_count_ = 0;
  size_t total_count_ = 0;

  ColumnStatsCollector(const ColumnStatsCollector&);
  void operator=(const ColumnStatsCollector&);
};

}  // namespace optimizer
}  // namespace peloton
