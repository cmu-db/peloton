//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "catalog/schema.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "storage/table_factory.h"
#include "common/value_factory.h"
#include "catalog/catalog_util.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

class Catalog {

 public:
  // Global Singleton
  static Catalog *GetInstance(void);

  Catalog();

  // Creates the catalog database
  void CreateCatalogDatabase(void);

  // Create a database
  Result CreateDatabase(std::string database_name,
                        concurrency::Transaction *txn);

  // Add a database
  void AddDatabase(storage::Database *database);

  // Create a table in a database
  Result CreateTable(std::string database_name, std::string table_name,
                     std::unique_ptr<catalog::Schema>,
                     concurrency::Transaction *txn);

  // Create the primary key index for a table
  Result CreatePrimaryIndex(const std::string &database_name,
                            const std::string &table_name);

  Result CreateIndex(const std::string &database_name,
                     const std::string &table_name,
                     std::vector<std::string> index_attr,
                     std::string index_name, bool unique, IndexType index_type);

  // Get a index with the oids of index, table, and database.
  index::Index *GetIndexWithOid(const oid_t database_oid, const oid_t table_oid,
                                const oid_t index_oid) const;
  // Drop a database
  Result DropDatabaseWithName(std::string database_name,
                              concurrency::Transaction *txn);

  // Drop a database with its oid
  void DropDatabaseWithOid(const oid_t database_oid);

  // Drop a table
  Result DropTable(std::string database_name, std::string table_name,
                   concurrency::Transaction *txn);

  // Find a database using its id
  storage::Database *GetDatabaseWithOid(const oid_t db_oid) const;

  // Find a database using its name
  storage::Database *GetDatabaseWithName(const std::string db_name) const;

  // Find a database using vector offset
  storage::Database *GetDatabaseWithOffset(const oid_t database_offset) const;

  // Create Table for pg_class
  std::unique_ptr<storage::DataTable> CreateTableCatalog(
      oid_t database_id, std::string table_name);

  // Create Table for pg_database
  std::unique_ptr<storage::DataTable> CreateDatabaseCatalog(
      oid_t database_id, std::string table_name);

  // Initialize the schema of the database catalog
  std::unique_ptr<Schema> InitializeDatabaseSchema();

  // Initialize the schema of the table catalog
  std::unique_ptr<Schema> InitializeTablesSchema();

  // Get table from a database with its name
  storage::DataTable *GetTableWithName(std::string database_name,
                                       std::string table_name);

  // Get table from a database with its oid
  storage::DataTable *GetTableWithOid(const oid_t database_oid,
                                      const oid_t table_oid) const;

  // Get the number of databases currently in the catalog
  oid_t GetDatabaseCount();

  void PrintCatalogs();

  // Get a new id for database, table, etc.
  oid_t GetNextOid();

  // Deconstruct the catalog database when destroy the catalog.
  ~Catalog();

 private:
  void InsertDatabaseIntoCatalogDatabase(oid_t database_id,
                                         std::string &database_name,
                                         concurrency::Transaction *txn);

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

  // The id variable that get assigned to objects. Initialized with (START_OID
  // +
  // 1) because START_OID is assigned to the catalog database.
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + 1);

  // Maximum Column Size for Catalog Schemas
  const size_t max_name_size_ = 32;
};
}
}
