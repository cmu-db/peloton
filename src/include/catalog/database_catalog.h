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
  DatabaseCatalogObject()
      : database_oid(INVALID_OID), database_name(), txn(nullptr) {}
  DatabaseCatalogObject(executor::LogicalTile *tile,
                        concurrency::Transaction *txn)
      : database_oid(tile->GetValue(0, 0).GetAs<oid_t>()),
        database_name(tile->GetValue(0, 1).ToString()),
        txn(txn) {}

  oid_t GetOid() const { return database_oid; }
  std::string GetDBName() const { return database_name; }

  bool InsertTableObject(std::shared_ptr<TableCatalogObject> table_object,
                         bool forced = false);
  bool EvictTableObject(const strd::string &table_name);
  std::shared_ptr<TableCatalogObject> GetTableObject(oid_t table_oid,
                                                     bool cached_only = false);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, bool cached_only = false);

  // database oid
  const oid_t database_oid;

  // database name
  std::string database_name;

  // private:
  // cache for table name to oid translation
  std::unordered_map<std::string, oid_t> table_name_cache;
  std::mutex table_cache_lock;

  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::Transaction *txn;
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
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::Transaction *txn);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
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
