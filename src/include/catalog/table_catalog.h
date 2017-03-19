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
// 0: table_id
// 1: table_name & database_name
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class TableCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static TableCatalog *GetInstance(void);

  inline oid_t GetNextOid() {
    return oid_++ | static_cast<oid_t>(type::CatalogType::TABLE);
  }

  // Write-related
  bool Insert(oid_t table_id, std::string table_name, oid_t database_id,
              std::string database_name, type::AbstractPool *pool,
              concurrency::Transaction *txn);

  bool DeleteByOid(oid_t id, concurrency::Transaction *txn);

  // TODO: combine all readonly functions with helper
  // Read-only
  std::string GetTableNameByOid(oid_t id, concurrency::Transaction *txn);

  std::string GetDatabaseNameByOid(oid_t id, concurrency::Transaction *txn);

  oid_t GetOidByName(std::string &table_name, std::string &database_name,
                     concurrency::Transaction *txn);

 private:
  TableCatalog();

  ~TableCatalog();

  std::unique_ptr<catalog::Schema> InitializeTableCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
