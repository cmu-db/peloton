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

class SchemaCatalogObject;
class TableCatalogObject;
class IndexCatalogObject;

class DatabaseCatalogObject {
  friend class DatabaseCatalog;
  friend class SchemaCatalog;
  friend class TableCatalog;
  friend class CatalogCache;

 public:
  DatabaseCatalogObject(executor::LogicalTile *tile,
                        concurrency::TransactionContext *txn);


  //===--------------------------------------------------------------------===//
  // Schema
  //===--------------------------------------------------------------------===//

  std::shared_ptr<SchemaCatalogObject> GetSchemaObject(oid_t namespace_oid);

  std::shared_ptr<SchemaCatalogObject> GetSchemaObject(const std::string &namespace_name);

  //===--------------------------------------------------------------------===//
  // Tables
  // FIXME: These should be moved into SchemaCatalogObject!
  //===--------------------------------------------------------------------===//


  void EvictAllTableObjects();
  std::shared_ptr<TableCatalogObject> GetTableObject(oid_t table_oid,
                                                     bool cached_only = false);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, const std::string &schema_name,
      bool cached_only = false);

  bool IsValidTableObjects() {
    // return true if this database object contains all table
    // objects within the database
    return valid_table_objects;
  }

  std::vector<std::shared_ptr<TableCatalogObject>> GetTableObjects(
      const std::string &schema_name);

  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
  GetTableObjects(bool cached_only = false);

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  oid_t GetDatabaseOid() const {
    return database_oid;
  }

  const std::string &GetDatabaseName() const {
    return database_name;
  }

 private:
  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::TransactionContext *txn;

  // member variables
  oid_t database_oid;
  std::string database_name;

  //===--------------------------------------------------------------------===//
  // Namespaces
  //===--------------------------------------------------------------------===//

  std::unordered_map<oid_t, std::shared_ptr<SchemaCatalogObject>>
      namespace_objects_cache_;

  //===--------------------------------------------------------------------===//
  // Tables
  // FIXME: These should be moved into SchemaCatalogObject!
  //===--------------------------------------------------------------------===//

  /**
   * @brief   insert table catalog object into cache
   * @param table_object
   * @return false if table_name already exists in cache
   */
  bool InsertTableObject(std::shared_ptr<TableCatalogObject> table_object);
  bool EvictTableObject(oid_t table_oid);
  bool EvictTableObject(const std::string &table_name,
                        const std::string &schema_name);
  void SetValidTableObjects(bool valid = true) { valid_table_objects = valid; }

  // cache for table name to oid translation
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
      table_objects_cache;
  std::unordered_map<std::string, std::shared_ptr<TableCatalogObject>>
      table_name_cache;
  bool valid_table_objects;

  //===--------------------------------------------------------------------===//
  // Indexes
  // FIXME: These should be moved into TableCatalogObject!
  //===--------------------------------------------------------------------===//

  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t index_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &index_name, const std::string &schema_name);
};

class DatabaseCatalog : public AbstractCatalog {
  friend class DatabaseCatalogObject;
  friend class TableCatalog;
  friend class CatalogCache;
  friend class Catalog;

 public:
  ~DatabaseCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static DatabaseCatalog *GetInstance(
      storage::Database *pg_catalog = nullptr,
      type::AbstractPool *pool = nullptr,
      concurrency::TransactionContext *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | DATABASE_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabase(oid_t database_oid, const std::string &database_name,
                      type::AbstractPool *pool,
                      concurrency::TransactionContext *txn);
  bool DeleteDatabase(oid_t database_oid, concurrency::TransactionContext *txn);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      oid_t database_oid, concurrency::TransactionContext *txn);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &database_name, concurrency::TransactionContext *txn);

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
