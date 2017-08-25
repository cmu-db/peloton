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

#include <mutex>

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class TableCatalogObject;
class IndexCatalogObject;

class DatabaseCatalogObject {
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class CatalogCache;

 public:
  DatabaseCatalogObject(oid_t database_oid = INVALID_OID)
      : database_oid(database_oid),
        database_name(),
        table_objects_cache(),
        table_name_cache(),
        valid_table_objects(false),
        txn(nullptr) {}
  DatabaseCatalogObject(executor::LogicalTile *tile,
                        concurrency::Transaction *txn)
      : database_oid(tile->GetValue(0, 0).GetAs<oid_t>()),
        database_name(tile->GetValue(0, 1).ToString()),
        txn(txn) {}

  void EvictAllTableObjects();
  std::shared_ptr<TableCatalogObject> GetTableObject(oid_t table_oid,
                                                     bool cached_only = false);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, bool cached_only = false);

  // oid_t GetOid() const { return database_oid; }
  // std::string GetDBName() const { return database_name; }
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t index_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &index_name);
  bool IsValidTableObjects() {
    // return true if this database object contains all table
    // objects within the database
    return valid_table_objects;
  }
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
  GetTableObjects(bool cached_only = false);

  // database oid
  const oid_t database_oid;

  // database name
  std::string database_name;

 private:
  bool InsertTableObject(std::shared_ptr<TableCatalogObject> table_object);
  bool EvictTableObject(oid_t table_oid);
  bool EvictTableObject(const std::string &table_name);
  void SetValidTableObjects(bool valid = true) { valid_table_objects = valid; }

  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t index_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &index_name);

  // cache for table name to oid translation
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
      table_objects_cache;
  std::unordered_map<std::string, std::shared_ptr<TableCatalogObject>>
      table_name_cache;
  // std::mutex table_cache_lock;

  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::Transaction *txn;
};

class DatabaseCatalog : public AbstractCatalog {
  friend class CatalogCache;
  friend class Catalog;
  friend class TableCatalog;

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

 private:
  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::Transaction *txn);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &database_name, concurrency::Transaction *txn);

  // Deprecated, use DatabaseCatalogObject
  // std::string GetDatabaseName(oid_t database_oid,
  //                             concurrency::Transaction *txn);
  // oid_t GetDatabaseOid(const std::string &database_name,
  //                      concurrency::Transaction *txn);

 private:
  DatabaseCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                  concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton
