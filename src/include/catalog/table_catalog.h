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

#include "catalog/abstract_catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "executor/logical_tile.h"
#include <unordered_map>

namespace peloton {
namespace catalog {

class TableCatalogObject {
 public:
  TableCatalogObject()
      : table_oid(INVALID_OID),
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

  oid_t GetOid() const { return table_oid; }
  std::string GetName() const { return table_name; }
  oid_t GetDatabaseOid() const { return database_oid; }
  std::unordered_map<oid_t, IndexCatalogObject> GetIndexObjects();
  std::unordered_map<size_t, ColumnCatalogObject> GetColumnObjects();

  // table oid
  oid_t table_oid;

  // table name
  std::string table_name;

  // database oid
  oid_t database_oid;

  std::unordered_map<oid_t, IndexCatalogObject> index_objects;
  std::unordered_map<size_t, ColumnCatalogObject> column_objects;

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
  const TableCatalogObject GetTableByOid(oid_t table_oid,
                                         concurrency::Transaction *txn);
  const TableCatalogObject GetTableByName(const std::string &table_name,
                                          oid_t database_oid,
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
