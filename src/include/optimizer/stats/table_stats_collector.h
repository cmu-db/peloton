//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats_collector.h
//
// Identification: src/include/optimizer/stats/table_stats_collector.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "optimizer/stats/column_stats_collector.h"
#include "catalog/schema.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// TableStatsCollector
//===--------------------------------------------------------------------===//
class TableStatsCollector {
 public:
  TableStatsCollector(storage::DataTable* table);

  ~TableStatsCollector();

  void CollectColumnStats();

  inline size_t GetActiveTupleCount() { return active_tuple_count_; }

  inline size_t GetColumnCount() { return column_count_; }

  ColumnStatsCollector* GetColumnStats(oid_t column_id);

 private:
  storage::DataTable* table_;
  catalog::Schema* schema_;
  std::vector<std::unique_ptr<ColumnStatsCollector>> column_stats_collectors_;
  size_t active_tuple_count_;
  size_t column_count_;

  TableStatsCollector(const TableStatsCollector&);
  void operator=(const TableStatsCollector&);

  void InitColumnStatsCollectors();
};

} /* namespace optimizer */
} /* namespace peloton */
