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

#include "optimizer/stats/table_stats.h"
#include "common/logger.h"
#include "optimizer/stats/column_stats.h"

namespace peloton {
namespace optimizer {

TableStats::TableStats(size_t num_rows,
                       std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
                       bool is_base_table)
    : Stats(nullptr),
      num_rows(num_rows),
      col_stats_list_(col_stats_ptrs),
      is_base_table_(is_base_table),
      tuple_sampler_{} {
  for (size_t i = 0; i < col_stats_ptrs.size(); ++i) {
    AddColumnStats(col_stats_ptrs[i]);
  }
}

TableStats::TableStats(std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
                       bool is_base_table)
    : Stats(nullptr),
      col_stats_list_(col_stats_ptrs),
      is_base_table_(is_base_table),
      tuple_sampler_{} {
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

void TableStats::UpdateNumRows(size_t new_num_rows) { 
  num_rows = new_num_rows; 
  for (auto& col_name_stats_pair : col_name_to_stats_map_) {
    auto& col_stats = col_name_stats_pair.second;
    col_stats->num_rows = num_rows;
  }
}

bool TableStats::AddColumnStats(std::shared_ptr<ColumnStats> col_stats) {
  auto it = col_name_to_stats_map_.find(col_stats->column_name);
  if (it != col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.insert({col_stats->column_name, col_stats});
  return true;
}

void TableStats::ClearColumnStats() { col_name_to_stats_map_.clear(); }

size_t TableStats::GetColumnCount() { return col_stats_list_.size(); }

bool TableStats::AddIndex(std::string key,
                          std::shared_ptr<index::Index> index_) {
  // Only consider adding single column index for now
  if (index_->GetColumnCount() > 1) return false;

  if (index_map_.find(key) == index_map_.end()) {
    index_map_.insert({key, index_});
    return true;
  }
  return false;
}

void TableStats::SampleTuples() {
  if (tuple_sampler_ == nullptr) return;
  tuple_sampler_->AcquireSampleTuples(DEFAULT_SAMPLE_SIZE);
}

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
// TableStats with column_name operations
//===--------------------------------------------------------------------===//
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

// Returns true if we have column stats for a specific column
bool TableStats::HasColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  return it != col_name_to_stats_map_.end();
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(
    const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it != col_name_to_stats_map_.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<index::Index> TableStats::GetIndex(std::string col_name) {
  if (index_map_.find(col_name) != index_map_.end()) {
    return index_map_.find(col_name)->second;
  }
  return std::shared_ptr<index::Index>(nullptr);
}

bool TableStats::RemoveColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.erase(col_name);
  return true;
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

void TableStats::UpdateJoinColumnStats(std::vector<oid_t> &column_ids) {

  std::unordered_map<oid_t, std::unordered_set<std::string>> distinct_values(
    column_ids.size());
  auto &samples = GetSampler()->GetSampledTuples();
  auto sample_size = samples.size();

  for (auto &sample : samples) {
    for (unsigned int column_id : column_ids) {
      distinct_values[column_id].insert(
        sample->GetValue(column_id).ToString());
    }
  }
  for (size_t i = 0; i < GetColumnCount(); i++) {
    auto column_stats = GetColumnStats(i);
    column_stats->UpdateJoinStats(num_rows, sample_size, distinct_values[column_stats->column_id].size());

  }
}


}  // namespace optimizer
}  // namespace peloton
