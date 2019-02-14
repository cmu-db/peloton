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

#include <sstream>

#include "common/macros.h"
#include "index/index.h"
#include "optimizer/stats/stats.h"
#include "common/internal_types.h"
#include "optimizer/stats/tuple_sampler.h"

namespace peloton {
namespace optimizer {

#define DEFAULT_CARDINALITY 1
#define DEFAULT_HAS_INDEX false

class ColumnStats;

//===--------------------------------------------------------------------===//
// TableStats
//===--------------------------------------------------------------------===//
class TableStats : public Stats {
 public:
  TableStats() : TableStats((size_t)0) {}

  TableStats(size_t num_rows, bool is_base_table = true)
      : Stats(nullptr),
        num_rows(num_rows),
        col_stats_list_{},
        col_name_to_stats_map_{},
        is_base_table_(is_base_table),
        tuple_sampler_{} {}

  TableStats(size_t num_rows,
             std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
             bool is_base_table = true);

  TableStats(std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
             bool is_base_table = true);

  /*
   * Right now table_stats need to support both column id and column name
   * lookup to support both base and intermediate table with alias.
   */

  void UpdateNumRows(size_t new_num_rows);

  bool HasIndex(const std::string column_name);

  bool HasIndex(const oid_t column_id);

  bool HasPrimaryIndex(const std::string column_name);

  bool HasPrimaryIndex(const oid_t column_id);

  double GetCardinality(const std::string column_name);

  double GetCardinality(const oid_t column_id);

  void ClearColumnStats();

  bool HasColumnStats(const std::string col_name);

  bool HasColumnStats(const oid_t column_id);

  std::shared_ptr<ColumnStats> GetColumnStats(const std::string col_name);

  std::shared_ptr<ColumnStats> GetColumnStats(const oid_t column_id);

  bool AddColumnStats(std::shared_ptr<ColumnStats> col_stats);

  bool RemoveColumnStats(const std::string col_name);

  bool RemoveColumnStats(const oid_t column_id);

  bool AddIndex(std::string key, const std::shared_ptr<index::Index> index);

  std::shared_ptr<index::Index> GetIndex(const std::string col_name);

  inline bool IsBaseTable() { return is_base_table_; }

  void SampleTuples();

  inline std::shared_ptr<TupleSampler> GetSampler() { return tuple_sampler_; }

  inline void SetTupleSampler(std::shared_ptr<TupleSampler> tuple_sampler) {
    tuple_sampler_ = tuple_sampler;
  }

  void UpdateJoinColumnStats(std::vector<oid_t> &column_ids);

  size_t GetColumnCount();

  std::string ToCSV();

  size_t num_rows;

 private:
  // TODO: only keep one ptr of ColumnStats
  std::vector<std::shared_ptr<ColumnStats>> col_stats_list_;
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
      col_name_to_stats_map_;
  std::unordered_map<std::string, std::shared_ptr<index::Index>> index_map_;
  bool is_base_table_;
  std::shared_ptr<TupleSampler> tuple_sampler_;

};

}  // namespace optimizer
}  // namespace peloton
