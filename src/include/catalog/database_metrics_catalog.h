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

#define DATABASE_METRICS_CATALOG_NAME "pg_database_metrics"

namespace peloton {
namespace catalog {

class DatabaseMetricsCatalogObject {
 public:
  // construct object from logical tile
  DatabaseMetricsCatalogObject(executor::LogicalTile *tile, int tupleID = 0);

  inline oid_t GetDatabaseOid() { return database_oid_; }
  inline int64_t GetTxnCommitted() { return txn_committed_; }
  inline int64_t GetTxnAborted() { return txn_aborted_; }
  inline int64_t GetTimeStamp() { return time_stamp_; }

 private:
  oid_t database_oid_;
  int64_t txn_committed_;
  int64_t txn_aborted_;
  int64_t time_stamp_;
};

class DatabaseMetricsCatalog : public AbstractCatalog {
  friend class DatabaseMetricsCatalogObject;

 public:
  ~DatabaseMetricsCatalog();

  // Global Singleton
  static DatabaseMetricsCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  inline std::string GetName() const override {
    return DATABASE_METRICS_CATALOG_NAME;
  }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabaseMetrics(oid_t database_oid, oid_t txn_committed,
                             oid_t txn_aborted, oid_t time_stamp,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn);

  bool DeleteDatabaseMetrics(oid_t database_oid,
                             concurrency::TransactionContext *txn);

  bool UpdateDatabaseMetrics(oid_t database_oid, oid_t txn_committed,
                             oid_t txn_aborted, oid_t time_stamp,
                             concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<DatabaseMetricsCatalogObject> GetDatabaseMetricsObject(
      oid_t database_oid, concurrency::TransactionContext *txn);

 private:
  enum ColumnId {
    DATABASE_OID = 0,
    TXN_COMMITTED = 1,
    TXN_ABORTED = 2,
    TIME_STAMP = 3,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3};

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };

  DatabaseMetricsCatalog(concurrency::TransactionContext *txn);
};

}  // namespace catalog
}  // namespace peloton
