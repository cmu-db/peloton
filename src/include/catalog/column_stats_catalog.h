//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats_catalog.h
//
// Identification: src/include/catalog/column_stats_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_column_stats
//
// Schema: (column offset: column_name)
// 0: table_id (pkey)
// 1: column_id (pkey)
// 2: num_rows
// 3: cardinality
// 4: frac_null
// 5: most_common_vals
// 6: most_common_freqs
// 7: histogram_bounds
//
// Indexes: (index offset: indexed columns)
// 0: table_id & column_id (unique)
// 1: table_id (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>

#include "catalog/abstract_catalog.h"

namespace peloton {

namespace optimizer {
class ColumnStats;
}

namespace catalog {

class ColumnStatsCatalog : public AbstractCatalog {
 public:

  ColumnStatsCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                     concurrency::TransactionContext *txn);

  ~ColumnStatsCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumnStats(oid_t table_id, oid_t column_id,
                         int num_rows, double cardinality, double frac_null,
                         std::string most_common_vals,
                         std::string most_common_freqs,
                         std::string histogram_bounds, std::string column_name,
                         bool has_index, type::AbstractPool *pool,
                         concurrency::TransactionContext *txn);
  bool DeleteColumnStats(oid_t table_id, oid_t column_id,
                         concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::unique_ptr<std::vector<type::Value>> GetColumnStats(
      oid_t table_id, oid_t column_id,
      concurrency::TransactionContext *txn);

  size_t GetTableStats(
          oid_t table_id, concurrency::TransactionContext *txn,
          std::map<oid_t, std::unique_ptr<std::vector<type::Value>>> &
          column_stats_map);
  // TODO: add more if needed


  /** @brief   private function for initialize schema of pg_index
   *  @return  unqiue pointer to schema
   */
  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_ID = 0,
    COLUMN_ID = 1,
    NUM_ROWS = 2,
    CARDINALITY = 3,
    FRAC_NULL = 4,
    MOST_COMMON_VALS = 5,
    MOST_COMMON_FREQS = 6,
    HISTOGRAM_BOUNDS = 7,
    COLUMN_NAME = 8,
    HAS_INDEX = 9,
    // Add new columns here in creation order
  };

  enum ColumnStatsOffset {
    NUM_ROWS_OFF = 0,
    CARDINALITY_OFF = 1,
    FRAC_NULL_OFF = 2,
    COMMON_VALS_OFF = 3,
    COMMON_FREQS_OFF = 4,
    HIST_BOUNDS_OFF = 5,
    COLUMN_NAME_OFF = 6,
    HAS_INDEX_OFF = 7,
  };

 private:

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    SECONDARY_KEY_1 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
