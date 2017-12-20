//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metrics_catalog.h
//
// Identification: src/include/catalog/table_metrics_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_table_metrics
//
// Schema: (column offset: column_name)
// 0: database_oid
// 1: table_oid
// 2: reads
// 3: updates
// 4: deletes
// 5: inserts
// 6: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "statistics/index_metric.h"

#define TABLE_METRICS_CATALOG_NAME "pg_table_metrics"

namespace peloton {
namespace catalog {

class TableMetricsCatalog : public AbstractCatalog {
 public:
  ~TableMetricsCatalog();

  // Global Singleton
  static TableMetricsCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTableMetrics(oid_t database_oid, oid_t table_oid, int64_t reads,
                          int64_t updates, int64_t deletes, int64_t inserts,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteTableMetrics(oid_t table_oid, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // TODO: add if needed

 private:
  TableMetricsCatalog(concurrency::TransactionContext *txn);

  enum ColumnId {
    DATABASE_OID = 0,
    TABLE_OID = 1,
    READS = 2,
    UPDATES = 3,
    DELETES = 4,
    INSERTS = 5,
    TIME_STAMP = 6,
    // Add new columns here in creation order
  };

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
