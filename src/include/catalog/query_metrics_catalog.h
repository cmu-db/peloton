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
// pg_query(per database, name become primary key)
//
// Schema: (column offset: column_name)
// 0: name (pkey)
// 1: num_params
// 2: param_types
// 3: param_formats
// 4: param_values
// 5: reads
// 6: updates
// 7: deletes
// 8: inserts
// 9: latency
// 10: cpu_time
// 11: time_stamp
//
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
  QueryMetricsCatalog(const std::string &database_name,
                      concurrency::TransactionContext *txn);
  ~QueryMetricsCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertQueryMetrics(const std::string &name, int64_t num_params,
                          const stats::QueryMetric::QueryParamBuf &type_buf,
                          const stats::QueryMetric::QueryParamBuf &format_buf,
                          const stats::QueryMetric::QueryParamBuf &value_buf,
                          int64_t reads, int64_t updates, int64_t deletes,
                          int64_t inserts, int64_t latency, int64_t cpu_time,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteQueryMetrics(const std::string &name,
                          concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  stats::QueryMetric::QueryParamBuf GetParamTypes(
      const std::string &name, concurrency::TransactionContext *txn);
  int64_t GetNumParams(const std::string &name,
                       concurrency::TransactionContext *txn);
  // TODO: add more if needed

  enum ColumnId {
    NAME = 0,
    NUM_PARAMS = 1,
    PARAM_TYPES = 2,
    PARAM_FORMATS = 3,
    PARAM_VALUES = 4,
    READS = 5,
    UPDATES = 6,
    DELETES = 7,
    INSERTS = 8,
    LATENCY = 9,
    CPU_TIME = 10,
    TIME_STAMP = 11,
    // Add new columns here in creation order
  };

 private:
  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
