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

  ResultType Insert(oid_t table_id, std::string table_name, oid_t database_id,
                    std::string database_name, type::AbstractPool *pool,
                    concurrency::Transaction *txn);

  ResultType DeleteByOid(oid_t id, concurrency::Transaction *txn);

  // Read-only
  std::string GetTableNameByOid(oid_t id, concurrency::Transaction *txn);
  std::string GetDatabaseNameByOid(oid_t id, concurrency::Transaction *txn);
  oid_t GetOidByName(std::string &table_name, std::string &database_name,
                     concurrency::Transaction *txn);

 private:
  TableCatalog();

  ~TableCatalog();

  std::unique_ptr<catalog::Schema> InitializeTableCatalogSchema();

  std::unique_ptr<storage::Tuple> CreateTableCatalogTuple(
      oid_t table_id, std::string table_name, oid_t database_id,
      std::string database_name, type::AbstractPool *pool);
};

}  // End catalog namespace
}  // End peloton namespace
