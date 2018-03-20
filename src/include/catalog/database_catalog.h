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
  DatabaseCatalogObject(executor::LogicalTile *tile,
                        concurrency::TransactionContext *txn);

  void EvictAllTableObjects();
  std::shared_ptr<TableCatalogObject> GetTableObject(oid_t table_oid,
                                                     bool cached_only = false);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, bool cached_only = false);

  bool IsValidTableObjects() {
    // return true if this database object contains all table
    // objects within the database
    return valid_table_objects;
  }
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
  GetTableObjects(bool cached_only = false);

  inline oid_t GetDatabaseOid() { return database_oid; }
  inline const std::string &GetDatabaseName() { return database_name; }

 private:
  // member variables
  oid_t database_oid;
  std::string database_name;

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
  bool valid_table_objects;

  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::TransactionContext *txn;
};

class DatabaseCatalog : public AbstractCatalog {
  friend class DatabaseCatalogObject;
  friend class TableCatalog;
  friend class CatalogCache;
  friend class Catalog;

 public:
  ~DatabaseCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static DatabaseCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                      type::AbstractPool *pool = nullptr,
                                      concurrency::TransactionContext *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | DATABASE_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabase(oid_t database_oid, const std::string &database_name,
                      type::AbstractPool *pool, concurrency::TransactionContext *txn);
  bool DeleteDatabase(oid_t database_oid, concurrency::TransactionContext *txn);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::TransactionContext *txn);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &database_name, concurrency::TransactionContext *txn);

 private:
  DatabaseCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                  concurrency::TransactionContext *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    DATABASE_OID = 0,
    DATABASE_NAME = 1,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_DATABASE_NAME = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
