#pragma once

#include <vector>
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

  // Not allow copy
  ColumnStats(const ColumnStats&);
  void operator=(const ColumnStats&);

  size_t null_count_ = 0;
  size_t total_count_ = 0; // <- just number of rows
  uint8_t num_bins = 5; // <- make it a parameter
};

} /* namespace optimizer */
} /* namespace peloton */
