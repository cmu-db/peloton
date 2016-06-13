//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/catalog/catalog.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "catalog/catalog.h"

#define CATALOG_DATABASE_NAME "catalog_db"
#define DATABASE_CATALOG_NAME "database_catalog"
#define TABLE_CATALOG_NAME    "table_catalog"

namespace peloton {
namespace catalog {

// Get instance of the global catalog
std::unique_ptr<Catalog> Catalog::GetInstance(void) {
  static std::unique_ptr<Catalog> global_catalog (new Catalog());
  return std::move(global_catalog);
}

// Creates the catalog database
void Catalog::CreateCatalogDatabase() {
  storage::Database *database = new storage::Database(START_OID);
  database->setDBName(CATALOG_DATABASE_NAME);
  auto database_catalog = CreateDatabaseCatalog(START_OID, DATABASE_CATALOG_NAME);
  storage::DataTable *table = database_catalog.release();
  database->AddTable(table);
  auto table_catalog = CreateTableCatalog(START_OID, TABLE_CATALOG_NAME);
  table = table_catalog.release();
  database->AddTable(table);
  databases.push_back(database);
}

// Create a database
void Catalog::CreateDatabase(std::string database_name) {
  oid_t database_id = GetNewID();
  storage::Database *database = new storage::Database(database_id);
  database->setDBName(database_name);
  databases.push_back(database);
  // Update catalog_db with this database info
  auto tuple = GetCatalogTuple(databases[START_OID]->GetTableWithName(DATABASE_CATALOG_NAME)->GetSchema(),
		  database_id,
		  database_name);
  InsertTuple(databases[START_OID]->GetTableWithName(DATABASE_CATALOG_NAME), std::move(tuple));
}

// Create a table in a database
void Catalog::CreateTable(oid_t database_id, std::string table_name, std::unique_ptr<catalog::Schema> schema) {
  bool own_schema = true;
  bool adapt_table = false;
  oid_t table_id = GetNewID();
  storage::DataTable *table = storage::TableFactory::GetDataTable(
		  database_id, table_id, schema.get(), table_name,
		  DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
  GetDatabaseWithOid(database_id)->AddTable(table);
  // Update catalog_table with this table info
  auto tuple = GetCatalogTuple(databases[START_OID]->GetTableWithName(TABLE_CATALOG_NAME)->GetSchema(), table_id, table_name);
  InsertTuple(databases[START_OID]->GetTableWithName(TABLE_CATALOG_NAME), std::move(tuple));
}

// Find a database using its id
storage::Database *Catalog::GetDatabaseWithOid(const oid_t db_oid) const {
  for (auto database : databases)
    if (database->GetOid() == db_oid) return database;
  return nullptr;
}

// Find a database using its name
storage::Database *Catalog::GetDatabaseWithName(const std::string database_name) const {
  for (auto database : databases)
    if (database->GetDBName() == database_name) return database;

  return nullptr;
}

// Create Table for pg_class
std::unique_ptr<storage::DataTable> Catalog::CreateTableCatalog(oid_t database_id, std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto table_schema = InitializeTablesSchema();

  catalog::Schema *schema = table_schema.release();
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
		  database_id, GetNewID(), schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table));
  return table;
}

// Create Table for pg_database
std::unique_ptr<storage::DataTable> Catalog::CreateDatabaseCatalog(oid_t database_id, std::string database_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto database_schema = InitializeDatabaseSchema();

  catalog::Schema *schema = database_schema.release();

  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
		  database_id, GetNewID(), schema, database_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table));

  return table;
}

// Initialize tables catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeTablesSchema() {
  const std::string not_null_constraint_name = "not_null";

  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "table_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, max_name_size,
                      "table_name", false);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  return table_schema;
}

// Initialize database catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeDatabaseSchema() {
  const std::string not_null_constraint_name = "not_null";

  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "database_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, max_name_size,
                      "database_name", false);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_schema(
      new catalog::Schema({id_column, name_column}));

  return database_schema;
}

int Catalog::GetDatabaseCount() { return databases.size(); }

oid_t Catalog::GetNewID() { return id_cntr++;}

}
}
