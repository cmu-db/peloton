//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.h
//
// Identification: src/include/catalog/query_metrics_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_query
//
// Schema: (column offset: column_name)
// 0: name (pkey)
// 1: database_oid (pkey)
// 2: num_params
// 3: param_types
// 4: param_formats
// 5: param_values
// 6: reads
// 7: updates
// 8: deletes
// 9: inserts
// 10: latency
// 11: cpu_time
// 12: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: name & database_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "statistics/query_metric.h"

#define QUERY_METRICS_CATALOG_NAME "pg_query_metrics"

namespace peloton {
namespace catalog {

class QueryMetricsCatalog : public AbstractCatalog {
 public:
  ~QueryMetricsCatalog();

  // Global Singleton
  static QueryMetricsCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertQueryMetrics(const std::string &name, oid_t database_oid,
                          int64_t num_params,
                          const stats::QueryMetric::QueryParamBuf &type_buf,
                          const stats::QueryMetric::QueryParamBuf &format_buf,
                          const stats::QueryMetric::QueryParamBuf &value_buf,
                          int64_t reads, int64_t updates, int64_t deletes,
                          int64_t inserts, int64_t latency, int64_t cpu_time,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteQueryMetrics(const std::string &name, oid_t database_oid,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  stats::QueryMetric::QueryParamBuf GetParamTypes(
      const std::string &name, oid_t database_oid,
      concurrency::TransactionContext *txn);
  int64_t GetNumParams(const std::string &name, oid_t database_oid,
                       concurrency::TransactionContext *txn);
  // TODO: add more if needed

  enum ColumnId {
    NAME = 0,
    DATABASE_OID = 1,
    NUM_PARAMS = 2,
    PARAM_TYPES = 3,
    PARAM_FORMATS = 4,
    PARAM_VALUES = 5,
    READS = 6,
    UPDATES = 7,
    DELETES = 8,
    INSERTS = 9,
    LATENCY = 10,
    CPU_TIME = 11,
    TIME_STAMP = 12,
    // Add new columns here in creation order
  };

 private:
  QueryMetricsCatalog(concurrency::TransactionContext *txn);

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
