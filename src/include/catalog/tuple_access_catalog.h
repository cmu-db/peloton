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
// tuple_access_metrics
//
// Schema: (column offset: column_name)
// 1: txn_id
// 2: arrival
// 3: reads
// 4: valid
// 5: committed
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "concurrency/transaction_context.h"

#define TUPLE_ACCESS_METRICS_CATALOG_NAME "tuple_access_metrics"

namespace peloton {
namespace catalog {

class TupleAccessMetricsCatalog : public AbstractCatalog {

 public:
  TupleAccessMetricsCatalog(const std::string &database_name,
                            concurrency::TransactionContext *txn);

  ~TupleAccessMetricsCatalog() = default;

  inline std::string GetName() const override {
    return TUPLE_ACCESS_METRICS_CATALOG_NAME;
  }

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertAccessMetric(txn_id_t tid,
                          int64_t arrival,
                          uint64_t reads,
                          bool valid,
                          bool committed,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteAccessMetrics(txn_id_t tid, int64_t arrival,
                           concurrency::TransactionContext *txn);

  bool UpdateAccessMetrics(txn_id_t tid,
                           int64_t arrival,
                           uint64_t reads,
                           bool valid,
                           bool committed,
                           concurrency::TransactionContext *txn);

 private:
  enum ColumnId {
    TXN_ID = 0,
    ARRIVAL = 1,
    READS = 2,
    VALID = 3,
    COMMITTED = 4
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4};

  enum IndexId {
    PRIMARY_KEY = 0,
    // under new hierarchical catalog design, each database has its own table
    // catalogs, so table_oid is a valid primary key
  };
};

}  // namespace catalog
}  // namespace peloton
