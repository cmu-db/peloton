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
// 1: table_oid
// 2: reads
// 3: updates
// 4: deletes
// 5: inserts
// 6: memory_alloc
// 7: memory_usage
// 8: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

#define TABLE_METRICS_CATALOG_NAME "pg_table_metrics"

namespace peloton {
namespace catalog {

class TableMetricsCatalogObject {
 public:
  TableMetricsCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetTableOid() { return table_oid_; }
  inline int64_t GetReads() { return reads_; }
  inline int64_t GetUpdates() { return updates_; }
  inline int64_t GetInserts() { return inserts_; }
  inline int64_t GetDeletes() { return deletes_; }
  inline int64_t GetMemoryAlloc() { return memory_alloc_; }
  inline int64_t GetMemoryUsage() { return memory_usage_; }
  inline int64_t GetTimeStamp() { return time_stamp_; }

 private:
  oid_t table_oid_;
  int64_t reads_;
  int64_t updates_;
  int64_t inserts_;
  int64_t deletes_;
  int64_t memory_alloc_;
  int64_t memory_usage_;
  int64_t time_stamp_;
};

class TableMetricsCatalog : public AbstractCatalog {
  friend class TableMetricsCatalogObject;

 public:
  TableMetricsCatalog(const std::string &database_name,
                      concurrency::TransactionContext *txn);
  ~TableMetricsCatalog();

  inline std::string GetName() const override {
    return TABLE_METRICS_CATALOG_NAME;
  }

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTableMetrics(oid_t table_oid, int64_t reads, int64_t updates,
                          int64_t inserts, int64_t deletes,
                          int64_t memory_alloc, int64_t memory_usage,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteTableMetrics(oid_t table_oid,
                          concurrency::TransactionContext *txn);

  bool UpdateTableMetrics(oid_t table_oid, int64_t reads, int64_t updates,
                          int64_t deletes, int64_t inserts,
                          int64_t memory_alloc, int64_t memory_usage,
                          int64_t time_stamp,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<TableMetricsCatalogObject> GetTableMetricsObject(
      oid_t table_oid, concurrency::TransactionContext *txn);

 private:
  enum ColumnId {
    TABLE_OID = 0,
    READS = 1,
    UPDATES = 2,
    INSERTS = 3,
    DELETES = 4,
    MEMORY_ALLOC = 5,
    MEMORY_USAGE = 6,
    TIME_STAMP = 7,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7};

  enum IndexId {
    PRIMARY_KEY = 0,
    // under new hierarchical catalog design, each database has its own table
    // catalogs, so table_oid is a valid primary key
  };
};

}  // namespace catalog
}  // namespace peloton
