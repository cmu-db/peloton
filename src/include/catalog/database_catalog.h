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
// Schema: (column offset: column_name)
// 0: database_oid (pkey)
// 1: database_name
//
// Indexes: (index offset: indexed columns)
// 0: database_oid (unique & primary key)
// 1: database_name (unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class DatabaseCatalogObject {
 public:
  DatabaseCatalogObject() : database_oid(INVALID_OID), database_name() {}
  DatabaseCatalogObject(executor::LogicalTile *tile)
      : database_oid(tile->GetValue(0, 0).GetAs<oid_t>()),
        database_name(tile->GetValue(0, 1).ToString()) {}

  oid_t GetOid() const { return database_oid; }
  std::string GetDBName() const { return database_name; }

  // database oid
  const oid_t database_oid;

  // database name
  std::string database_name;
};

class DatabaseCatalog : public AbstractCatalog {
 public:
  ~DatabaseCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static DatabaseCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                      type::AbstractPool *pool = nullptr,
                                      concurrency::Transaction *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | DATABASE_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabase(oid_t database_oid, const std::string &database_name,
                      type::AbstractPool *pool, concurrency::Transaction *txn);
  bool DeleteDatabase(oid_t database_oid, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  const DatabaseCatalogObject GetDatabaseObjectByOid(
      oid_t database_oid, concurrency::Transaction *txn);
  const DatabaseCatalogObject GetDatabaseObjectByName(
      const std::string &database_name, concurrency::Transaction *txn);

  // Deprecated, use DatabaseCatalogObject
  std::string GetDatabaseName(oid_t database_oid,
                              concurrency::Transaction *txn);
  oid_t GetDatabaseOid(const std::string &database_name,
                       concurrency::Transaction *txn);

 private:
  DatabaseCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                  concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton
