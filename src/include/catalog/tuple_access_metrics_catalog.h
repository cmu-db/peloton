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
// 2: reads
// 3: valid
// 4: committed
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

class TupleAccessMetricsCatalogObject {
 public:
  TupleAccessMetricsCatalogObject(executor::LogicalTile *tile, oid_t tupleId = 0);

  inline txn_id_t GetTxnId() { return tid_; }
  inline uint64_t GetReads() { return reads_; }
  inline bool IsValid() { return valid_; }
  inline bool IsCommitted() { return committed_; }

 private:
  txn_id_t tid_;
  uint64_t reads_;
  bool valid_;
  bool committed_;
};

class TupleAccessMetricsCatalog : public AbstractCatalog {
  friend class TupleAccessMetricsCatalogObject;
 public:

  static TupleAccessMetricsCatalog *GetInstance(concurrency::TransactionContext *txn) {
    static TupleAccessMetricsCatalog catalog{txn};
    return &catalog;
  }
  TupleAccessMetricsCatalog(concurrency::TransactionContext *txn);

  ~TupleAccessMetricsCatalog() = default;

  inline std::string GetName() const override {
    return TUPLE_ACCESS_METRICS_CATALOG_NAME;
  }

  //===--------------------------------------------------------------------===//
  // Write Related API
  //===--------------------------------------------------------------------===//
  bool InsertAccessMetric(txn_id_t tid,
                          uint64_t reads,
                          bool valid,
                          bool committed,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);
  bool DeleteAccessMetrics(txn_id_t tid,
                           concurrency::TransactionContext *txn);

  bool UpdateAccessMetrics(txn_id_t tid,
                           uint64_t reads,
                           bool valid,
                           bool committed,
                           concurrency::TransactionContext *txn);
  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<TupleAccessMetricsCatalogObject> GetTupleAccessMetricsCatalogObject(
      txn_id_t tid,
      concurrency::TransactionContext *txn);

 private:
  enum ColumnId {
    TXN_ID = 0,
    READS = 1,
    VALID = 2,
    COMMITTED = 3
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3};

  enum IndexId {
    PRIMARY_KEY = 0,
    // under new hierarchical catalog design, each database has its own table
    // catalogs, so table_oid is a valid primary key
  };
};

}  // namespace catalog
}  // namespace peloton
