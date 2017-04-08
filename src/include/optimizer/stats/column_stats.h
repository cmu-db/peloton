#pragma once

#include <vector>

#include "type/types.h"
#include "optimizer/stats/count_min_sketch.h"
#include "optimizer/stats/histogram.h"
#include "optimizer/stats/distinct_value_counter.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStats
//===--------------------------------------------------------------------===//
class ColumnStats {
public:
  using ValueFrequencyPair = std::pair<type::Value, double>;

  ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id, type::Type::TypeId column_type);

  ~ColumnStats();

  void AddValue(type::Value value);

  double GetNullValueCount();

  std::vector<ValueFrequencyPair> GetCommonValueAndFrequency();

  double GetCardinality();

  std::vector<type::Value> GetHistogramBound(uint8_t bin_nums);

private:
  const oid_t database_id_;
  const oid_t table_id_;
  const oid_t column_id_;
  const type::Type::TypeId column_type_;

  DistinctValueCounter *distinct_value_counter_;

  // Not allow copy
  ColumnStats(const ColumnStats&);
  void operator=(const ColumnStats&);

  void InitStatsCollectors();
};

} /* namespace optimizer */
} /* namespace peloton */
