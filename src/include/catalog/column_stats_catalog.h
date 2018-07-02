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
// 0: database_id (pkey)
// 1: table_id (pkey)
// 2: column_id (pkey)
// 3: num_rows
// 4: cardinality
// 5: frac_null
// 6: most_common_vals
// 7: most_common_freqs
// 8: histogram_bounds
//
// Indexes: (index offset: indexed columns)
// 0: name & database_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>

#include "catalog/abstract_catalog.h"

#define COLUMN_STATS_CATALOG_NAME "pg_column_stats"

namespace peloton {

namespace optimizer {
class ColumnStats;
}

namespace catalog {

class ColumnStatsCatalog : public AbstractCatalog {
 public:
  ColumnStatsCatalog(concurrency::TransactionContext *txn,
                     const std::string &database_name);

  ~ColumnStatsCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumnStats(concurrency::TransactionContext *txn,
                         oid_t table_id,
                         oid_t column_id,
                         std::string column_name,
                         int num_rows,
                         double frac_null,
                         std::string most_common_vals,
                         std::string most_common_freqs,
                         std::string histogram_bounds,
                         double cardinality,
                         bool has_index,
                         type::AbstractPool *pool);

  bool DeleteColumnStats(concurrency::TransactionContext *txn,
                         oid_t table_id,
                         oid_t column_id);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::unique_ptr<std::vector<type::Value>> GetColumnStats(concurrency::TransactionContext *txn,
                                                           oid_t table_id,
                                                           oid_t column_id);

  size_t GetTableStats(concurrency::TransactionContext *txn,
                       oid_t table_id,
                       std::map<oid_t, std::unique_ptr<std::vector<type::Value>>>
                           &column_stats_map);

  // TODO: add more if needed
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

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    SECONDARY_KEY_1 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
