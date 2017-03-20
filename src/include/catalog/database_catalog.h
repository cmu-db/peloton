//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.h
//
// Identification: src/include/catalog/database_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_database
//
// Schema: (column: column_name)
// 0: database_id (pkey), 1: database_name
//
// Indexes: (index offset: indexed columns)
// 0: database_id
// 1: database_name
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class DatabaseCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static DatabaseCatalog *GetInstance(void);

  inline oid_t GetNextOid() {
    return oid_++ | static_cast<oid_t>(type::CatalogType::DATABASE);
  }

  // Write related API
  bool Insert(oid_t database_id, const std::string &database_name,
              concurrency::Transaction *txn);
  void DeleteByOid(oid_t database_id, concurrency::Transaction *txn);

  // Read-only API
  std::string GetNameByOid(oid_t database_id, concurrency::Transaction *txn);
  oid_t GetOidByName(const std::string &database_name, concurrency::Transaction *txn);

 private:
  DatabaseCatalog();

  ~DatabaseCatalog();

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
