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
#include "common/logger.h"

namespace peloton {
namespace optimizer {

TableStats::TableStats(size_t num_rows,
                       std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs)
    : num_rows(num_rows), col_stats_list_(col_stats_ptrs) {
  for (size_t i = 0; i < col_stats_ptrs.size(); ++i) {
    AddColumnStats(col_stats_ptrs[i]);
  }
}

TableStats::TableStats(std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs)
    : col_stats_list_(col_stats_ptrs) {
  size_t col_count = col_stats_ptrs.size();
  for (size_t i = 0; i < col_count; ++i) {
    AddColumnStats(col_stats_ptrs[i]);
  }
  if (col_count == 0) {
    num_rows = 0;
  } else {
    num_rows = col_stats_ptrs[0]->num_rows;
  }
}

void TableStats::UpdateNumRows(size_t new_num_rows) { num_rows = new_num_rows; }

bool TableStats::HasIndex(const std::string column_name) {
  auto column_stats = GetColumnStats(column_name);
  if (column_stats == nullptr) {
    return DEFAULT_HAS_INDEX;
  }
  return column_stats->has_index;
}

bool TableStats::HasPrimaryIndex(const std::string column_name) {
  return HasIndex(column_name);
}

double TableStats::GetCardinality(const std::string column_name) {
  auto column_stats = GetColumnStats(column_name);
  if (column_stats == nullptr) {
    return DEFAULT_CARDINALITY;
  }
  return column_stats->cardinality;
}

void TableStats::ClearColumnStats() { col_name_to_stats_map_.clear(); }

bool TableStats::HasColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  return true;
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(
    const std::string col_name) {
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

bool TableStats::RemoveColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.erase(col_name);
  return true;
}

size_t TableStats::GetColumnCount() { return col_stats_list_.size(); }

std::string TableStats::ToCSV() {
  std::ostringstream os;
  os << "\n"
     << "===[TableStats]===\n";
  os << "column_id|column_name|num_rows|has_index|cardinality|"
     << "frac_null|most_common_freqs|most_common_vals|histogram_bounds\n";
  for (auto column_stats : col_stats_list_) {
    os << column_stats->ToCSV();
  }
  return os.str();
}

//===--------------------------------------------------------------------===//
// TableStats with column_id operations
//===--------------------------------------------------------------------===//
bool TableStats::HasIndex(const oid_t column_id) {
  auto column_stats = GetColumnStats(column_id);
  if (column_stats == nullptr) {
    return false;
  }
  return column_stats->has_index;
}

// Update this function once we support primary index operations.
bool TableStats::HasPrimaryIndex(const oid_t column_id) {
  return HasIndex(column_id);
}

double TableStats::GetCardinality(const oid_t column_id) {
  auto column_stats = GetColumnStats(column_id);
  if (column_stats == nullptr) {
    return DEFAULT_CARDINALITY;
  }
  return column_stats->cardinality;
}

bool TableStats::HasColumnStats(const oid_t column_id) {
  return column_id < col_stats_list_.size();
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(const oid_t column_id) {
  if (column_id >= col_stats_list_.size()) {
    return nullptr;
  }
  return col_stats_list_[column_id];
}

bool TableStats::RemoveColumnStats(const oid_t column_id) {
  if (column_id >= col_stats_list_.size()) {
    return false;
  }
  col_stats_list_.erase(col_stats_list_.begin() + column_id);
  return true;
}

}  // namespace optimizer
}  // namespace peloton
