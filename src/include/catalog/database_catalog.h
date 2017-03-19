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

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class DatabaseCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static DatabaseCatalog *GetInstance(void);

  DatabaseCatalog();

  ~DatabaseCatalog();

  inline oid_t GetNextOid() { return oid_++ | static_cast<oid_t>(type::CatalogType::DATABASE); }

  // Write related API
  void DeleteByOid(oid_t id, concurrency::Transaction *txn);
  bool Insert(
    oid_t database_id, std::string &database_name,
    type::AbstractPool *pool, concurrency::Transaction *txn);

  // Read-only API
  std::string GetNameByOid(oid_t id, concurrency::Transaction *txn);
  oid_t GetOidByName(std::string &name, concurrency::Transaction *txn);


 private:
  std::unique_ptr<catalog::Schema> InitializeDatabaseCatalogSchema();
  executor::LogicalTile *GetXByOid(oid_t id, int idx, concurrency::Transaction *txn);
  executor::LogicalTile *GetXByName(std::string &name, int idx, concurrency::Transaction *txn);
};

}  // End catalog namespace
}  // End peloton namespace
