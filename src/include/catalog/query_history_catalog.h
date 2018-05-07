//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_history_catalog.h
//
// Identification: src/include/catalog/query_history_catalog.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "type/ephemeral_pool.h"

#define QUERY_HISTORY_CATALOG_NAME "pg_query_history"

namespace peloton {
namespace catalog {

class QueryHistoryCatalog : public AbstractCatalog {
 public:
  ~QueryHistoryCatalog();

  // Global Singleton
  static QueryHistoryCatalog &GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertQueryHistory(const std::string &query_string,
                          const std::string &fingerprint, uint64_t timestamp,
                          type::AbstractPool *pool,
                          concurrency::TransactionContext *txn);

  std::unique_ptr<std::vector<std::pair<uint64_t, std::string>>>
  GetQueryStringsAfterTimestamp(const uint64_t start_timestamp,
                                concurrency::TransactionContext *txn);

  enum ColumnId {
    QUERY_STRING = 0,
    FINGERPRINT = 1,
    TIMESTAMP = 2,
  };

 private:
  QueryHistoryCatalog(concurrency::TransactionContext *txn);

  // Pool to use for variable length strings
  type::EphemeralPool pool_;

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
