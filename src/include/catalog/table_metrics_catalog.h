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
  TableMetricsCatalog(const std::string &database_name,
                      concurrency::TransactionContext *txn);
  ~TableMetricsCatalog();

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTableMetrics(oid_t table_oid, int64_t reads, int64_t updates,
                          int64_t deletes, int64_t inserts, int64_t time_stamp,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteTableMetrics(oid_t table_oid,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // TODO: add if needed

 private:
  enum ColumnId {
    TABLE_OID = 0,
    READS = 1,
    UPDATES = 2,
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
