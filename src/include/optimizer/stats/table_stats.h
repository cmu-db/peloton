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
#include "optimizer/stats.h"
#include "common/internal_types.h"

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

  TableStats(size_t num_rows)
      : Stats(nullptr),
        num_rows(num_rows),
        col_stats_list_{},
        col_name_to_stats_map_{} {}

  TableStats(size_t num_rows,
             std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs);

  TableStats(std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs);

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

  size_t GetColumnCount();

  std::string ToCSV();

  size_t num_rows;

 private:
  // TODO: only keep one ptr of ColumnStats
  std::vector<std::shared_ptr<ColumnStats>> col_stats_list_;
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
      col_name_to_stats_map_;
};

}  // namespace optimizer
}  // namespace peloton
