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
  IndexMetricsCatalog(const std::string &database_name,
                      concurrency::TransactionContext *txn);
  ~IndexMetricsCatalog();

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertIndexMetrics(oid_t table_oid, oid_t index_oid, int64_t reads,
                          int64_t deletes, int64_t inserts, int64_t time_stamp,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteIndexMetrics(oid_t index_oid,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // TODO: add if needed

 private:
  enum ColumnId {
    TABLE_OID = 0,
    INDEX_OID = 1,
    READS = 2,
    DELETES = 3,
    INSERTS = 4,
    TIME_STAMP = 5,
    // Add new columns here in creation order
  };

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
