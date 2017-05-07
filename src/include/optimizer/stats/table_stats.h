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

#include "common/macros.h"
#include "type/types.h"

namespace peloton {
namespace optimizer {

class ColumnStats;

//===--------------------------------------------------------------------===//
// TableStats
//===--------------------------------------------------------------------===//
class TableStats {
 public:
  TableStats() : num_rows(0) {}

  TableStats(size_t num_rows) : num_rows(num_rows) {}

  TableStats(size_t num_rows,
             std::vector<std::shared_ptr<ColumnStats>> col_stats_list);

  size_t num_rows;

  bool HasColumnStats(std::string col_name);

  std::shared_ptr<ColumnStats> GetColumnStats(std::string col_name);

  bool AddColumnStats(std::shared_ptr<ColumnStats> col_stats);

  bool RemoveColumnStats(std::string col_name);

 private:
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
      col_name_to_stats_map_;
};

} /* namespace optimizer */
} /* namespace peloton */
