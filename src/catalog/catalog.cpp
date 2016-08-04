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
#include "catalog/manager.h"
#include "index/index_factory.h"

#define CATALOG_DATABASE_NAME "catalog_db"
#define DATABASE_CATALOG_NAME "database_catalog"
#define TABLE_CATALOG_NAME "table_catalog"

namespace peloton {
namespace catalog {

// Get instance of the global catalog
std::unique_ptr<Catalog> Catalog::GetInstance(void) {
  static std::unique_ptr<Catalog> global_catalog(new Catalog());
  return std::move(global_catalog);
}

// Creates the catalog database
void Catalog::CreateCatalogDatabase() {
  storage::Database *database = new storage::Database(START_OID);
  database->setDBName(CATALOG_DATABASE_NAME);
  auto database_catalog =
      CreateDatabaseCatalog(START_OID, DATABASE_CATALOG_NAME);
  storage::DataTable *databases_table = database_catalog.release();
  database->AddTable(databases_table);
  auto table_catalog = CreateTableCatalog(START_OID, TABLE_CATALOG_NAME);
  storage::DataTable *tables_table = table_catalog.release();
  database->AddTable(tables_table);
  databases.push_back(database);
}

// Create a database
Result Catalog::CreateDatabase(std::string database_name) {
  // Check if a database with the same name exists
  for (auto database : databases) {
    if (database->GetDBName() == database_name) {
      LOG_TRACE("Database already exists. Returning RESULT_FAILURE.");
      return Result::RESULT_FAILURE;
    }
  }
  oid_t database_id = GetNewID();
  storage::Database *database = new storage::Database(database_id);
  database->setDBName(database_name);
  databases.push_back(database);
  // Update catalog_db with this database info

  auto tuple = GetDatabaseCatalogTuple(databases[START_OID]->GetTableWithName(DATABASE_CATALOG_NAME)->GetSchema(),
      database_id,
      database_name);
  catalog::InsertTuple(databases[START_OID]->GetTableWithName(DATABASE_CATALOG_NAME), std::move(tuple));

  LOG_TRACE("Database created. Returning RESULT_SUCCESS.");
  return Result::RESULT_SUCCESS;
}

// Create a table in a database
Result Catalog::CreateTable(std::string database_name, std::string table_name,
                            std::unique_ptr<catalog::Schema> schema) {
  bool own_schema = true;
  bool adapt_table = false;
  oid_t table_id = GetNewID();
  storage::Database *database = GetDatabaseWithName(database_name);
  if (database != nullptr) {
    if (database->GetTableWithName(table_name)) {
      LOG_TRACE("Found a table with the same name. Returning RESULT_FAILURE");
      return Result::RESULT_FAILURE;
    }
    oid_t database_id = database->GetOid();
    storage::DataTable *table = storage::TableFactory::GetDataTable(
        database_id, table_id, schema.release(), table_name,
        DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
    GetDatabaseWithOid(database_id)->AddTable(table);

    // Create the primary key index for that table
    CreatePrimaryIndex(database_name, table_name);

    // Update catalog_table with this table info
    auto tuple = GetTableCatalogTuple(
        databases[START_OID]->GetTableWithName(TABLE_CATALOG_NAME)->GetSchema(),
        table_id, table_name, database_id, database->GetDBName());
    //  Another way of insertion using transaction manager
    catalog::InsertTuple(
        databases[START_OID]->GetTableWithName(TABLE_CATALOG_NAME),
        std::move(tuple));
    return Result::RESULT_SUCCESS;
  } else {
    LOG_TRACE("Could not find a database with name %s", database_name.c_str());
    return Result::RESULT_FAILURE;
  }
}

Result Catalog::CreatePrimaryIndex(const std::string &database_name,
                                   const std::string &table_name) {
  LOG_TRACE("Trying to create primary index for table %s", table_name.c_str());
  storage::Database *database = GetDatabaseWithName(database_name);
  if (database) {
    auto table = database->GetTableWithName(table_name);
    if (table == nullptr) {
      LOG_TRACE(
          "Cannot find the table to create the primary key index. Return "
          "RESULT_FAILURE.");
      return Result::RESULT_FAILURE;
    }

    std::vector<oid_t> key_attrs;
    catalog::Schema *key_schema = nullptr;
    index::IndexMetadata *index_metadata = nullptr;
    auto schema = table->GetSchema();

    // Find primary index attributes
    for (auto &column : schema->GetColumns()) {
      if (column.IsPrimary()) {
        key_attrs.push_back(column.column_offset);
      }
    }

    key_schema = catalog::Schema::CopySchema(schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    index_metadata = new index::IndexMetadata(
        "customer_pkey", Manager::GetInstance().GetNextOid(), INDEX_TYPE_SKIPLIST,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, schema, key_schema, key_attrs, true);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetInstance(index_metadata));
    table->AddIndex(pkey_index);

    LOG_TRACE("Successfully add primary key index for table %s",
             table->GetName().c_str());
    return Result::RESULT_SUCCESS;
  } else {
    LOG_TRACE("Could not find a database with name %s", database_name.c_str());
    return Result::RESULT_FAILURE;
  }
}


Result Catalog::CreateIndex(const std::string &database_name,
                                   const std::string &table_name , std::vector<std::string> index_attr, std::string index_name , bool unique) {

  auto database = GetDatabaseWithName(database_name);
  if(database != nullptr){
     auto table = database->GetTableWithName(table_name);
        if (table == nullptr) {
          LOG_TRACE(
              "Cannot find the table to create the primary key index. Return "
              "RESULT_FAILURE.");
          return Result::RESULT_FAILURE;
        }

        std::vector<oid_t> key_attrs;
        catalog::Schema *key_schema = nullptr;
        index::IndexMetadata *index_metadata = nullptr;
        auto schema = table->GetSchema();

        // check if index attributes are in table
        auto columns = schema->GetColumns();
        for(auto attr : index_attr){
          for(uint i = 0; i < columns.size() ; ++i){  
            if(attr == columns[i].column_name){

              key_attrs.push_back(columns[i].column_offset);
            }
          }
        }
        
        if(key_attrs.size() != index_attr.size()){

          LOG_TRACE("Some columns are missing");
          return Result::RESULT_FAILURE;
        }

        key_schema = catalog::Schema::CopySchema(schema, key_attrs);
        key_schema->SetIndexedColumns(key_attrs);

        if(unique){
        index_metadata = new index::IndexMetadata(
                index_name.c_str(), Manager::GetInstance().GetNextOid(), INDEX_TYPE_SKIPLIST,
                INDEX_CONSTRAINT_TYPE_DEFAULT, schema, key_schema, key_attrs, true);
        }

        else {
        index_metadata = new index::IndexMetadata(
                index_name.c_str(), Manager::GetInstance().GetNextOid(), INDEX_TYPE_SKIPLIST,
                INDEX_CONSTRAINT_TYPE_UNIQUE, schema, key_schema, key_attrs, true); 
        }

        std::shared_ptr<index::Index> key_index(
             index::IndexFactory::GetInstance(index_metadata));
        table->AddIndex(key_index);

        LOG_TRACE("Successfully add index for table %s",
                 table->GetName().c_str());
       return Result::RESULT_SUCCESS;
  }

  return Result::RESULT_FAILURE;
}

// Drop a database
Result Catalog::DropDatabase(std::string database_name) {
  LOG_TRACE("Dropping database %s", database_name.c_str());
  storage::Database *database = GetDatabaseWithName(database_name);
  if (database != nullptr) {
    LOG_TRACE("Found database!");
    LOG_TRACE("Deleting tuple from catalog");
    catalog::DeleteTuple(GetDatabaseWithName(CATALOG_DATABASE_NAME)
                             ->GetTableWithName(DATABASE_CATALOG_NAME),
                         database->GetOid());
    oid_t database_offset = 0;
    for (auto database : databases) {
      if (database->GetDBName() == database_name) {
        LOG_TRACE("Deleting database object in database vector");
        delete database;
        break;
      }
      database_offset++;
    }
    PL_ASSERT(database_offset < databases.size());
    // Drop the database
    LOG_TRACE("Deleting database from database vector");
    databases.erase(databases.begin() + database_offset);
  } else {
    LOG_TRACE("Database is not found!");
    return Result::RESULT_FAILURE;
  }
  return Result::RESULT_SUCCESS;
}

// Drop a table
Result Catalog::DropTable(std::string database_name, std::string table_name) {

  LOG_TRACE("Dropping table %s from database %s", table_name.c_str(),
           database_name.c_str());
  storage::Database *database = GetDatabaseWithName(database_name);
  if (database != nullptr) {
    LOG_TRACE("Found database!");
    storage::DataTable *table = database->GetTableWithName(table_name);
    if (table) {
      LOG_TRACE("Found table!");
      oid_t table_id = table->GetOid();
      LOG_TRACE("Deleting tuple from catalog!");
      catalog::DeleteTuple(GetDatabaseWithName(CATALOG_DATABASE_NAME)
                               ->GetTableWithName(TABLE_CATALOG_NAME),
                           table_id);
      LOG_TRACE("Deleting table!");
      database->DropTableWithOid(table_id);
      return Result::RESULT_SUCCESS;
    } else {
      LOG_TRACE("Could not find table");
      return Result::RESULT_FAILURE;
    }
  } else {
    LOG_TRACE("Could not find database");
    return Result::RESULT_FAILURE;
  }
}

// Find a database using its id
storage::Database *Catalog::GetDatabaseWithOid(const oid_t db_oid) const {
  for (auto database : databases)
    if (database->GetOid() == db_oid) return database;
  return nullptr;
}

// Find a database using its name
storage::Database *Catalog::GetDatabaseWithName(const std::string database_name)
    const {
  for (auto database : databases) {
    if (database->GetDBName() == database_name) return database;
  }
  return nullptr;
}

// Get table from a database
storage::DataTable *Catalog::GetTableFromDatabase(std::string database_name,
                                                  std::string table_name) {
  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
           database_name.c_str());
  storage::Database *database = GetDatabaseWithName(database_name);
  if (database != nullptr) {
    storage::DataTable *table = database->GetTableWithName(table_name);
    if (table) {
      LOG_TRACE("Found table.");
      return table;
    } else {
      LOG_TRACE("Couldn't find table.");
      return nullptr;
    }
  } else {
    LOG_TRACE("Well, database wasn't found in the first place.");
    return nullptr;
  }
}

// Create Table for pg_class
std::unique_ptr<storage::DataTable> Catalog::CreateTableCatalog(
    oid_t database_id, std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto table_schema = InitializeTablesSchema();

  catalog::Schema *schema = table_schema.release();
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      database_id, GetNewID(), schema, table_name, DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema, adapt_table));
  return table;
}

// Create Table for pg_database
std::unique_ptr<storage::DataTable> Catalog::CreateDatabaseCatalog(
    oid_t database_id, std::string database_name) {
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

  auto id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "table_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, max_name_size, "table_name", true);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto database_name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, max_name_size, "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(
      {id_column, name_column, database_id_column, database_name_column}));

  return table_schema;
}

// Initialize database catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeDatabaseSchema() {
  const std::string not_null_constraint_name = "not_null";

  auto id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "database_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, max_name_size, "database_name", true);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_schema(
      new catalog::Schema({id_column, name_column}));
  return database_schema;
}

void Catalog::PrintCatalogs() {}

int Catalog::GetDatabaseCount() { return databases.size(); }

oid_t Catalog::GetNewID() { return id_cntr++; }
}
}
