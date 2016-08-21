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
  static std::unique_ptr<Catalog> GetInstance(void);

  // Creates the catalog database
  void CreateCatalogDatabase(void);

  // Create a database
  Result CreateDatabase(std::string database_name, concurrency::Transaction *txn);

  // Create a table in a database
  Result CreateTable(std::string database_name, std::string table_name,
                     std::unique_ptr<catalog::Schema>, concurrency::Transaction *txn);

  // Create the primary key index for a table
  Result CreatePrimaryIndex(const std::string &database_name,
                            const std::string &table_name);

  Result CreateIndex(const std::string &database_name,
                     const std::string &table_name,
                     std::vector<std::string> index_attr,
                     std::string index_name, bool unique, IndexType index_type);

  // Drop a database
  Result DropDatabase(std::string database_name, concurrency::Transaction *txn);

  // Drop a table
  Result DropTable(std::string database_name, std::string table_name, concurrency::Transaction *txn);

  // Find a database using its id
  storage::Database *GetDatabaseWithOid(const oid_t db_oid) const;

  // Find a database using its name
  storage::Database *GetDatabaseWithName(const std::string db_name) const;

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

  // Get table from a database
  storage::DataTable *GetTableFromDatabase(std::string database_name,
                                           std::string table_name);

  // Get the number of databases currently in the catalog
  int GetDatabaseCount();

  void PrintCatalogs();

  // Get a new id for database, table, etc.
  oid_t GetNewID();

  // Deconstruct the catalog database when destroy the catalog.
  ~Catalog();

 private:
  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases;

  // The id variable that get assigned to objects. Initialized with START_OID
  oid_t id_cntr = 1;

  // Mutex used for atomic operations
  std::mutex catalog_mutex;

  // Maximum Column Size for Catalog Schemas
  const size_t max_name_size = 32;
};
}
}
