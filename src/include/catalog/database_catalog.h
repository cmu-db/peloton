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

  void DeleteTuple(oid_t id, concurrency::Transaction *txn);

  std::unique_ptr<storage::Tuple> DatabaseCatalog::GetDatabaseCatalogTuple(
    oid_t database_id, std::string database_name,
    type::AbstractPool *pool);

 private:
  std::unique_ptr<catalog::Schema> InitializeDatabaseCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
