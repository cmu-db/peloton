//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.h
//
// Identification: src/include/storage/database.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <mutex>

#include "common/printable.h"
#include "storage/data_table.h"

struct ExecuteResult;
struct dirty_table_info;
struct dirty_index_info;

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

class Database : public Printable {
 public:
  Database() = delete;
  Database(Database const &) = delete;

  Database(const oid_t database_oid);

  ~Database();

  //===--------------------------------------------------------------------===//
  // OPERATIONS
  //===--------------------------------------------------------------------===//

  oid_t GetOid() const { return database_oid; }

  //===--------------------------------------------------------------------===//
  // TABLE
  //===--------------------------------------------------------------------===//

  void AddTable(storage::DataTable *table, bool is_catalog = false);

  storage::DataTable *GetTable(const oid_t table_offset) const;

  // Throw CatalogException if such table is not found
  storage::DataTable *GetTableWithOid(const oid_t table_oid) const;

  // Throw CatalogException if such table is not found
  storage::DataTable *GetTableWithName(const std::string &table_name) const;

  oid_t GetTableCount() const;

  void DropTableWithOid(const oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
  std::string GetDBName();
  void setDBName(const std::string &database_name);

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database oid
  const oid_t database_oid;

  // database name
  // TODO: deprecated, use
  // catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
  std::string database_name;

  // TABLES
  std::vector<storage::DataTable *> tables;

  std::mutex database_mutex;
};

}  // End storage namespace
}  // End peloton namespace
