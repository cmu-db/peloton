//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog/index_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_index
//
// Schema: (column: column_name)
// 0: index_oid (pkey), 1: index_name, 2: table_oid, 3: database_oid, 4: unique_keys
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique)
// 1: index_name & table_oid & database_oid (unique)
// 2: table_oid & database_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class IndexCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static IndexCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                   type::AbstractPool *pool = nullptr);

  inline oid_t GetNextOid() { return oid_++ | INDEX_OID_MASK; }

  // Write related API
  bool InsertIndex(oid_t index_oid, std::string &index_name, oid_t table_oid, oid_t database_oid, bool unique_keys);
  bool DeleteIndex(oid_t index_oid, concurrency::Transaction *txn);
  bool DeleteIndexes(oid_t database_oid, oid_t table_oid, concurrency::Transaction *txn);

  // Read-only API
  std::string GetIndexName(oid_t index_oid, concurrency::Transaction *txn);
  oid_t GetTableOid(oid_t index_oid, concurrency::Transaction *txn);
  oid_t GetDatabaseOid(oid_t index_oid, concurrency::Transaction *txn);
  bool GetUniquekeys(oid_t index_oid, concurrency::Transaction *txn);

  std::vector<oid_t> GetIndexOids(oid_t table_oid, oid_t database_oid, concurrency::Transaction *txn);

 private:
  IndexCatalog();

  ~IndexCatalog();

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
