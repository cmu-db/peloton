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
// Schema: (column position: column_name)
// 0: table_oid (pkey)
// 1: table_name,
// 2: database_oid(the database oid that this table belongs to)
//
// Indexes: (index offset: indexed columns)
// 0: table_oid (unique & primary key)
// 1: table_name & database_oid (unique)
// 2: database_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <unordered_map>

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class IndexCatalogObject;
class ColumnCatalogObject;

class TableCatalogObject {
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class ColumnCatalog;

 public:
  TableCatalogObject(executor::LogicalTile *tile, concurrency::TransactionContext *txn,
                     int tupleId = 0);

 public:
  // Get indexes
  void EvictAllIndexObjects();
  std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
  GetIndexObjects(bool cached_only = false);
  std::unordered_map<std::string, std::shared_ptr<IndexCatalogObject>>
  GetIndexNames(bool cached_only = false);
  std::shared_ptr<IndexCatalogObject> GetIndexObject(oid_t index_oid,
                                                     bool cached_only = false);
  std::shared_ptr<IndexCatalogObject> GetIndexObject(
      const std::string &index_name, bool cached_only = false);

  // Get columns
  void EvictAllColumnObjects();
  std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
  GetColumnObjects(bool cached_only = false);
  std::unordered_map<std::string, std::shared_ptr<ColumnCatalogObject>>
  GetColumnNames(bool cached_only = false);
  std::shared_ptr<ColumnCatalogObject> GetColumnObject(
      oid_t column_id, bool cached_only = false);
  std::shared_ptr<ColumnCatalogObject> GetColumnObject(
      const std::string &column_name, bool cached_only = false);

  inline oid_t GetTableOid() { return table_oid; }
  inline const std::string &GetTableName() { return table_name; }
  inline oid_t GetDatabaseOid() { return database_oid; }

 private:
  // member variables
  oid_t table_oid;
  std::string table_name;
  oid_t database_oid;

  // Get index objects
  bool InsertIndexObject(std::shared_ptr<IndexCatalogObject> index_object);
  bool EvictIndexObject(oid_t index_oid);
  bool EvictIndexObject(const std::string &index_name);

  // Get column objects
  bool InsertColumnObject(std::shared_ptr<ColumnCatalogObject> column_object);
  bool EvictColumnObject(oid_t column_id);
  bool EvictColumnObject(const std::string &column_name);

  // cache for *all* index catalog objects in this table
  std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>> index_objects;
  std::unordered_map<std::string, std::shared_ptr<IndexCatalogObject>>
      index_names;
  bool valid_index_objects;

  // cache for *all* column catalog objects in this table
  std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
      column_objects;
  std::unordered_map<std::string, std::shared_ptr<ColumnCatalogObject>>
      column_names;
  bool valid_column_objects;

  // Pointer to its corresponding transaction
  concurrency::TransactionContext *txn;
};

class TableCatalog : public AbstractCatalog {
  friend class TableCatalogObject;
  friend class DatabaseCatalogObject;
  friend class ColumnCatalog;
  friend class IndexCatalog;
  friend class Catalog;

 public:
  ~TableCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static TableCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                   type::AbstractPool *pool = nullptr,
                                   concurrency::TransactionContext *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | TABLE_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTable(oid_t table_oid, const std::string &table_name,
                   oid_t database_oid, type::AbstractPool *pool,
                   concurrency::TransactionContext *txn);
  bool DeleteTable(oid_t table_oid, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
 private:
  std::shared_ptr<TableCatalogObject> GetTableObject(
      oid_t table_oid, concurrency::TransactionContext *txn);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, oid_t database_oid,
      concurrency::TransactionContext *txn);
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
  GetTableObjects(oid_t database_oid, concurrency::TransactionContext *txn);

 private:
  TableCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
               concurrency::TransactionContext *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    TABLE_NAME = 1,
    DATABASE_OID = 2,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_NAME = 1,
    SKEY_DATABASE_OID = 2,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
