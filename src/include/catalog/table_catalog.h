//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.h
//
// Identification: src/include/catalog/table_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_table
//
// Schema: (column: column_name)
// 0: table_oid (pkey), 1: table_name, 2: database_oid
//
// Indexes: (index offset: indexed columns)
// 0: table_oid (unique)
// 1: table_name & database_oid (unique)
// 2: database_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class TableCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static TableCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                   type::AbstractPool *pool = nullptr);

  inline oid_t GetNextOid() { return oid_++ | TABLE_OID_MASK; }

  // Write related API
  bool InsertTable(oid_t table_oid, const std::string &table_name,
                   oid_t database_oid, type::AbstractPool *pool,
                   concurrency::Transaction *txn);
  bool DeleteTable(oid_t table_oid, concurrency::Transaction *txn);

  // Read-only API
  std::string GetTableName(oid_t table_oid, concurrency::Transaction *txn);
  oid_t GetDatabaseoid(oid_t table_oid, concurrency::Transaction *txn);
  oid_t GetTableOid(const std::string &table_name,
                    oid_t database_oid,
                    concurrency::Transaction *txn);
  std::vector<oid_t> GetTableOids(oid_t database_oid,
                                  concurrency::Transaction *txn);
  std::vector<std::string> GetTableNames(oid_t database_oid,
                                         concurrency::Transaction *txn);

 private:
  TableCatalog(storage::Database *pg_catalog, type::AbstractPool *pool);

  ~TableCatalog();

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
