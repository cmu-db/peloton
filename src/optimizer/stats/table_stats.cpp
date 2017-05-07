//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats.cpp
//
// Identification: src/optimizer/stats/table_stats.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/table_stats.h"

namespace peloton {
namespace optimizer {

TableStats::TableStats(size_t num_row,
                       std::vector<std::shared_ptr<ColumnStats>> col_stats_list)
    : num_row(num_row) {
  for (size_t i = 0; i < col_stats_list.size(); ++i) {
    AddColumnStats(col_stats_list[i]);
  }
}

bool TableStats::HasColumnStats(std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  return true;
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it != col_name_to_stats_map_.end()) {
    return it->second;
  }
  return nullptr;
}

bool TableStats::AddColumnStats(std::shared_ptr<ColumnStats> col_stats) {
  auto it = col_name_to_stats_map_.find(col_stats->column_name);
  if (it != col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.insert({col_stats->column_name, col_stats});
  return true;
}

bool TableStats::RemoveColumnStats(std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.erase(col_name);
  return true;
}

}  // namespace optimizer
}  // namespace peloton
