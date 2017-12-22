//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metrics_catalog.h
//
// Identification: src/include/catalog/database_metrics_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
//
// Schema: (column offset: column_name)
// 0: database_oid (pkey)
// 1: txn_committed
// 2: txn_aborted
// 3: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: database_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "statistics/database_metric.h"

#define DATABASE_METRICS_CATALOG_NAME "pg_database_metrics"

namespace peloton {
namespace catalog {

class DatabaseMetricsCatalog : public AbstractCatalog {
 public:
  ~DatabaseMetricsCatalog();

  // Global Singleton
  static DatabaseMetricsCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabaseMetrics(oid_t database_oid, oid_t txn_committed,
                             oid_t txn_aborted, oid_t time_stamp,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn);
  bool DeleteDatabaseMetrics(oid_t database_oid, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  oid_t GetTimeStamp(oid_t database_oid, concurrency::TransactionContext *txn);
  // TODO: add more if needed

  enum ColumnId {
    DATABASE_OID = 0,
    TXN_COMMITTED = 1,
    TXN_ABORTED = 2,
    TIME_STAMP = 3,
    // Add new columns here in creation order
  };

 private:
  DatabaseMetricsCatalog(concurrency::TransactionContext *txn);

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
