//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metrics_catalog.h
//
// Identification: src/include/catalog/index_metrics_catalog.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_index_metrics
//
// Schema: (column offset: column_name)
// 0: index_oid
// 1: table_oid
// 2: reads
// 3: updates
// 4: inserts
// 5: deletes
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
#include "statistics/index_metric.h"

#define INDEX_METRICS_CATALOG_NAME "pg_index_metrics"

namespace peloton {
namespace catalog {

// helper class for reading tuples from catalog
class IndexMetricsCatalogObject {
 public:
  IndexMetricsCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetIndexOid() { return index_oid_; }
  inline oid_t GetTableOid() { return table_oid_; }
  inline int64_t GetReads() { return reads_; }
  inline int64_t GetUpdates() { return updates_; }
  inline int64_t GetInserts() { return inserts_; }
  inline int64_t GetDeletes() { return deletes_; }
  inline int64_t GetMemoryAlloc() { return memory_alloc_; }
  inline int64_t GetMemoryUsage() { return memory_usage_; }
  inline int64_t GetTimeStamp() { return time_stamp_; }

 private:
  oid_t index_oid_;
  oid_t table_oid_;
  int64_t reads_;
  int64_t updates_;
  int64_t inserts_;
  int64_t deletes_;
  int64_t memory_alloc_;
  int64_t memory_usage_;
  int64_t time_stamp_;
};

class IndexMetricsCatalog : public AbstractCatalog {
  friend class IndexMetricsCatalogObject;

 public:
  IndexMetricsCatalog(const std::string &database_name,
                      concurrency::TransactionContext *txn);
  ~IndexMetricsCatalog();

  inline std::string GetName() const override {
    return INDEX_METRICS_CATALOG_NAME;
  }

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertIndexMetrics(oid_t index_oid, oid_t table_oid, int64_t reads,
                          int64_t updates, int64_t inserts, int64_t deletes,
                          int64_t memory_alloc, int64_t memory_usage,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);

  bool DeleteIndexMetrics(oid_t index_oid,
                          concurrency::TransactionContext *txn);

  bool UpdateIndexMetrics(oid_t index_oid, oid_t table_oid, int64_t reads,
                          int64_t updates, int64_t inserts, int64_t deletes,
                          int64_t memory_alloc, int64_t memory_usage,
                          int64_t time_stamp,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<IndexMetricsCatalogObject> GetIndexMetricsObject(
      oid_t index_oid, concurrency::TransactionContext *txn);

 private:
  enum ColumnId {
    INDEX_OID = 0,
    TABLE_OID = 1,
    READS = 2,
    UPDATES = 3,
    INSERTS = 4,
    DELETES = 5,
    MEMORY_ALLOC = 6,
    MEMORY_USAGE = 7,
    TIME_STAMP = 8,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7, 8};

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
