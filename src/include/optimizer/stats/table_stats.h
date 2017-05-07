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
  TableStats() : num_row(0) {}

  TableStats(size_t num_row) : num_row(num_row) {}

  TableStats(size_t num_row,
             std::vector<std::shared_ptr<ColumnStats>> col_stats_list);

  size_t num_row;

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
