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
// 0: table_id (pkey), 1: table_name, 2: database_id, 3: database_name
//
// Indexes: (index offset: indexed columns)
// 0: table_id (unique)
// 1: table_name & database_name (unique)
// 2: database_id (non-unique)
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
  bool InsertTable(oid_t table_id, const std::string &table_name,
                   oid_t database_id, const std::string &database_name,
                   type::AbstractPool *pool, concurrency::Transaction *txn);
  bool DeleteTable(oid_t table_id, concurrency::Transaction *txn);
  bool DeleteTables(oid_t database_id, concurrency::Transaction *txn);

  // Read-only API
  std::string GetTableName(oid_t table_id, concurrency::Transaction *txn);
  std::string GetDatabaseName(oid_t table_id, concurrency::Transaction *txn);
  oid_t GetTableId(const std::string &table_name,
                   const std::string &database_name,
                   concurrency::Transaction *txn);
  std::vector<oid_t> GetTableIds(oid_t database_id,
                                 concurrency::Transaction *txn);
  std::vector<std::string> GetTableNames(oid_t database_id,
                                         concurrency::Transaction *txn);

 private:
  TableCatalog(storage::Database *pg_catalog, type::AbstractPool *pool);

  ~TableCatalog();

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
