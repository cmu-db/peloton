//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metrics_catalog.h
//
// Identification: src/include/catalog/index_metrics_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_index_metrics
//
// Schema: (column offset: column_name)
// 0: database_oid
// 1: table_oid
// 2: index_oid
// 3: reads
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

#define INDEX_METRICS_CATALOG_NAME "pg_index_metrics"

namespace peloton {
namespace catalog {

class IndexMetricsCatalog : public AbstractCatalog {
 public:
  ~IndexMetricsCatalog();

  // Global Singleton
  static IndexMetricsCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertIndexMetrics(oid_t database_oid, oid_t table_oid, oid_t index_oid,
                          int64_t reads, int64_t deletes, int64_t inserts,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteIndexMetrics(oid_t index_oid, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // TODO: add if needed

 private:
  IndexMetricsCatalog(concurrency::TransactionContext *txn);

  enum ColumnId {
    DATABASE_OID = 0,
    TABLE_OID = 1,
    INDEX_OID = 2,
    READS = 3,
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
