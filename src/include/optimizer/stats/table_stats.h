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

  size_t GetActiveTupleCount();

  size_t GetColumnCount();

  ColumnStats* GetColumnStats(oid_t column_id);

  std::vector<ColumnStats*>& GetAllColumnStats();

private:
  storage::DataTable* table_;
	catalog::Schema* schema_;
  std::vector<std::unique_ptr<ColumnStats>> column_stats_;
  size_t active_tuple_count_;
  size_t column_count_;

  // Not allow copy
  TableStats(const TableStats&);
  void operator=(const TableStats&);

  void InitColumnStats();
};

} /* namespace optimizer */
} /* namespace peloton */
