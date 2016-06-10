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

#include "catalog/catalog.h"

namespace peloton {
namespace catalog {

Catalog &Catalog::GetInstance(void) {
  static Catalog global_catalog;
  return global_catalog;
}

void Catalog::AddDatabase(std::string database_name) {
{
  std::lock_guard<std::mutex> lock(catalog_mutex);
  storage::Database *database = new storage::Database(GetNewID());
  database->setDBName(database_name);
  databases.push_back(database);
}
}

storage::Database *Catalog::GetDatabaseWithOid(const oid_t db_oid) const {
  for (auto database : databases)
    if (database->GetOid() == db_oid) return database;
  return nullptr;
}

storage::Database *Catalog::GetDatabaseWithName(const std::string database_name) const {
  for (auto database : databases)
    if (database->GetDBName() == database_name) return database;

  return nullptr;
}

// Create Table for pg_class
storage::DataTable *Catalog::CreateTableCatalog(std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto table_schema = InitializeTablesSchema();
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, GetNewID(), table_schema.get(), table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  return table;
}

// Create Table for pg_database
storage::DataTable *Catalog::CreateDatabaseCatalog(std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto database_schema = InitializeDatabaseSchema();
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, GetNewID(), database_schema.get(), table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  return table;
}

// Initialize tables catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeTablesSchema() {
  const bool is_inlined = true;
  const std::string not_null_constraint_name = "not_null";

  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "table_id", is_inlined);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, GetTypeSize(VALUE_TYPE_VARCHAR),
                      "table_name", is_inlined);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  return table_schema;
}

// Initialize database catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeDatabaseSchema() {
  const bool is_inlined = true;
  const std::string not_null_constraint_name = "not_null";

  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "database_id", is_inlined);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, GetTypeSize(VALUE_TYPE_VARCHAR),
                      "database_name", is_inlined);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_schema(
      new catalog::Schema({id_column, name_column}));

  return database_schema;
}


oid_t Catalog::GetNewID() { return id_cntr++;}

}
}
