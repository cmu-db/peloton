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

namespace peloton {
namespace catalog {

class DatabaseMetricsCatalog : public AbstractCatalog {
 public:
  ~DatabaseMetricsCatalog();

  // Global Singleton
  static DatabaseMetricsCatalog *GetInstance(
      storage::Database *pg_catalog = nullptr,
      type::AbstractPool *pool = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool DatabaseMetricsCatalog::InsertDatabaseMetrics(
      oid_t database_oid, oid_t txn_committed, oid_t txn_aborted,
      oid_t time_stamp, concurrency::Transaction *txn);
  bool DatabaseMetricsCatalog::DeleteDatabaseMetrics(
      oid_t database_oid, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  oid_t DatabaseMetricsCatalog::GetTimeStamp(oid_t database_oid,
                                             concurrency::Transaction *txn);
  // TODO: add more if needed

  enum ColumnId {
    DATABASE_OID = 0,
    TXN_COMMITTED = 1,
    TXN_ABORTED = 2,
    TIME_STAMP = 3,
    // Add new columns here in creation order
  };

 private:
  DatabaseMetricsCatalog(storage::Database *pg_catalog,
                         type::AbstractPool *pool);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // End catalog namespace
}  // End peloton namespace
