//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats.h
//
// Identification: src/include/optimizer/stats/table_stats.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "optimizer/stats/column_stats.h"
#include "catalog/schema.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// TableStats
//===--------------------------------------------------------------------===//
class TableStats {
public:
  TableStats(storage::DataTable* table);

  ~TableStats();

  void CollectColumnStats();

  inline size_t GetActiveTupleCount() {
    return active_tuple_count_;
  }

  inline size_t GetColumnCount() {
    return column_count_;
  }

  ColumnStats* GetColumnStats(oid_t column_id);

private:
  storage::DataTable* table_;
	catalog::Schema* schema_;
  std::vector<std::unique_ptr<ColumnStats>> column_stats_;
  size_t active_tuple_count_;
  size_t column_count_;

  TableStats(const TableStats&);
  void operator=(const TableStats&);

  void InitColumnStats();
};

} /* namespace optimizer */
} /* namespace peloton */
