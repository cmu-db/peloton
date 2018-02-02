//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_history_catalog.h
//
// Identification: src/include/catalog/query_history_catalog.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_query
//
// Schema: (column offset: column_name)
// 0: query_string
// 1: fingerprint
// 2: timestamp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

#define QUERY_HISTORY_CATALOG_NAME "pg_query_history"

namespace peloton {
namespace catalog {

class QueryHistoryCatalog : public AbstractCatalog {
 public:
  ~QueryHistoryCatalog();

  // Global Singleton
  static QueryHistoryCatalog *GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertQueryHistory(const std::string &query_string,
                          std::string &fingerprint, uint64_t timestamp,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);

  enum ColumnId {
    QUERY_STRING = 0,
    FINGERPRINT = 1,
    TIMESTAMP = 2,
  };

 private:
  QueryHistoryCatalog(concurrency::TransactionContext *txn);

};

}  // namespace catalog
}  // namespace peloton
