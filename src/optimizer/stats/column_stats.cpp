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
      hll_{hll_precision},
      hist_{max_bins},
      sketch_{cmsketch_eps, cmsketch_gamma, 0},
      topk_{sketch_, top_k} {}

ColumnStats::~ColumnStats() {}

void ColumnStats::AddValue(const type::Value& value) {
  if (value.GetTypeId() != column_type_) {
    LOG_TRACE(
      "Incompatible value type %d with expected column stats value type %d",
      value.GetTypeId(), column_type_);
    return;
  }
  total_count_++;
  if (value.IsNull()) {
    null_count_++;
  }
  // Update all stats
  hll_.Update(value);
  hist_.Update(value);
  topk_.Add(value);
}

double ColumnStats::GetFracNull() {
  if (total_count_ == 0) {
    LOG_TRACE("Cannot calculate stats for table size 0.");
    return 0;
  }
  return (static_cast<double>(null_count_) / total_count_);
}

} /* namespace optimizer */
} /* namespace peloton */
