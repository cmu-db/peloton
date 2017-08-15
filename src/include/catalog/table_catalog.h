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
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class TableCatalogObject {
 public:
  TableCatalogObject(oid_t table_oid = INVALID_OID)
      : table_oid(table_oid),  // INVALID_OID represents an invalid object
        table_name(),
        database_oid(INVALID_OID),
        index_objects(),
        column_objects(),
        valid_index_objects(false),
        valid_column_objects(false),
        txn(nullptr) {}
  TableCatalogObject(executor::LogicalTile *tile, concurrency::Transaction *txn)
      : table_oid(tile->GetValue(0, 0).GetAs<oid_t>()),
        table_name(tile->GetValue(0, 1).ToString()),
        database_oid(tile->GetValue(0, 2).GetAs<oid_t>()),
        index_objects(),
        column_objects(),
        valid_index_objects(false),
        valid_column_objects(false),
        txn(txn) {}

  // Get index objects
  std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>> &
  GetIndexObjects();
  std::shared_ptr<IndexCatalogObject> GetIndexObject(oid_t index_oid);

  // Get column objects
  std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>> &
  GetColumnObjects();
  std::shared_ptr<ColumnCatalogObject> GetColumnObject(oid_t column_id);
  std::shared_ptr<ColumnCatalogObject> GetColumnObject(
      const std::string &column_name);

  oid_t GetOid() const { return table_oid; }
  std::string GetName() const { return table_name; }
  oid_t GetDatabaseOid() const { return database_oid; }

  // table oid
  const oid_t table_oid;

  // table name
  std::string table_name;

  // database oid
  oid_t database_oid;

 private:
  std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>> index_objects;
  // std::mutex index_cache_lock;

  std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
      column_objects;
  std::unordered_map<std::string, oid_t> column_name_cache;
  // std::mutex column_cache_lock;

  bool valid_index_objects;
  bool valid_column_objects;

  // Pointer to its corresponding transaction
  concurrency::Transaction *txn;
};

class TableCatalog : public AbstractCatalog {
 public:
  ~TableCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static TableCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                   type::AbstractPool *pool = nullptr,
                                   concurrency::Transaction *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | TABLE_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTable(oid_t table_oid, const std::string &table_name,
                   oid_t database_oid, type::AbstractPool *pool,
                   concurrency::Transaction *txn);
  bool DeleteTable(oid_t table_oid, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<TableCatalogObject> GetTableObject(
      oid_t table_oid, concurrency::Transaction *txn);
  std::shared_ptr<TableCatalogObject> GetTableObject(
      const std::string &table_name, oid_t database_oid,
      concurrency::Transaction *txn);

  std::string GetTableName(oid_t table_oid, concurrency::Transaction *txn);
  oid_t GetDatabaseOid(oid_t table_oid, concurrency::Transaction *txn);
  oid_t GetTableOid(const std::string &table_name, oid_t database_oid,
                    concurrency::Transaction *txn);
  std::vector<oid_t> GetTableOids(oid_t database_oid,
                                  concurrency::Transaction *txn);
  std::vector<std::string> GetTableNames(oid_t database_oid,
                                         concurrency::Transaction *txn);

 private:
  TableCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
               concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton
