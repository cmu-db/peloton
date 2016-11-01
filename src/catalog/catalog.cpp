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
#include "common/exception.h"
#include "common/macros.h"
#include "index/index_factory.h"

namespace peloton {
namespace catalog {

// Get instance of the global catalog
Catalog *Catalog::GetInstance(void) {
  static std::unique_ptr<Catalog> global_catalog(new Catalog());
  return global_catalog.get();
}

Catalog::Catalog() {
  CreateCatalogDatabase();

  // Create metrics table in default database
  CreateMetricsCatalog();
}

void Catalog::CreateMetricsCatalog() {
  auto default_db = GetDatabaseWithName(CATALOG_DATABASE_NAME);
  auto default_db_oid = default_db->GetOid();

  // Create table for database metrics
  auto database_metrics_catalog =
      CreateMetricsCatalog(default_db_oid, DATABASE_METRIC_NAME);
  default_db->AddTable(database_metrics_catalog.release());

  // Create table for index metrics
  auto index_metrics_catalog =
      CreateMetricsCatalog(default_db_oid, INDEX_METRIC_NAME);
  default_db->AddTable(index_metrics_catalog.release());

  // Create table for table metrics
  auto table_metrics_catalog =
      CreateMetricsCatalog(default_db_oid, TABLE_METRIC_NAME);
  default_db->AddTable(table_metrics_catalog.release());

  // Create table for query metrics
  auto query_metrics_catalog =
      CreateMetricsCatalog(default_db_oid, QUERY_METRIC_NAME);
  default_db->AddTable(query_metrics_catalog.release());
  LOG_DEBUG("Metrics tables created");
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
  databases_.push_back(database);
  LOG_DEBUG("Catalog database created");
}

// Create a database
Result Catalog::CreateDatabase(std::string database_name,
                               concurrency::Transaction *txn) {
  // Check if a database with the same name exists
  for (auto database : databases_) {
    if (database->GetDBName() == database_name) {
      LOG_TRACE("Database already exists. Returning RESULT_FAILURE.");
      return Result::RESULT_FAILURE;
    }
  }
  oid_t database_id = GetNextOid();
  storage::Database *database = new storage::Database(database_id);
  database->setDBName(database_name);
  databases_.push_back(database);

  InsertDatabaseIntoCatalogDatabase(database_id, database_name, txn);

  LOG_TRACE("Database created. Returning RESULT_SUCCESS.");
  return Result::RESULT_SUCCESS;
}

void Catalog::AddDatabase(storage::Database *database) {
  databases_.push_back(database);
  std::string database_name;
  InsertDatabaseIntoCatalogDatabase(database->GetOid(), database_name, nullptr);
}

void Catalog::InsertDatabaseIntoCatalogDatabase(oid_t database_id,
                                                std::string &database_name,
                                                concurrency::Transaction *txn) {
  // Update catalog_db with this database info
  auto tuple =
      GetDatabaseCatalogTuple(databases_[START_OID]
                                  ->GetTableWithName(DATABASE_CATALOG_NAME)
                                  ->GetSchema(),
                              database_id, database_name, pool_);
  catalog::InsertTuple(
      databases_[START_OID]->GetTableWithName(DATABASE_CATALOG_NAME),
      std::move(tuple), txn);
}

// Create a table in a database
Result Catalog::CreateTable(std::string database_name, std::string table_name,
                            std::unique_ptr<catalog::Schema> schema,
                            concurrency::Transaction *txn) {
  LOG_TRACE("Creating table %s in database %s", table_name.c_str(),
            database_name.c_str());

  bool own_schema = true;
  bool adapt_table = false;
  oid_t table_id = GetNextOid();
  try {
    storage::Database *database = GetDatabaseWithName(database_name);
    try {
      database->GetTableWithName(table_name);
      LOG_TRACE("Found a table with the same name. Returning RESULT_FAILURE");
      return Result::RESULT_FAILURE;
    }
    catch (CatalogException &e) {
      // Table doesn't exist, now create it
      oid_t database_id = database->GetOid();
      storage::DataTable *table = storage::TableFactory::GetDataTable(
          database_id, table_id, schema.release(), table_name,
          DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
      GetDatabaseWithOid(database_id)->AddTable(table);

      // Create the primary key index for that table if there's primary key
      bool has_primary_key = false;
      auto &schema_columns = table->GetSchema()->GetColumns();
      for (auto &column : schema_columns)
        if (column.IsPrimary()) {
          has_primary_key = true;
          break;
        }
      if (has_primary_key == true)
        CreatePrimaryIndex(database_name, table_name);

      // Update catalog_table with this table info
      auto tuple = GetTableCatalogTuple(
          databases_[START_OID]
              ->GetTableWithName(TABLE_CATALOG_NAME)
              ->GetSchema(),
          table_id, table_name, database_id, database->GetDBName(), pool_);
      // Another way of insertion using transaction manager
      catalog::InsertTuple(
          databases_[START_OID]->GetTableWithName(TABLE_CATALOG_NAME),
          std::move(tuple), txn);
      return Result::RESULT_SUCCESS;
    }
  }
  catch (CatalogException &e) {
    LOG_ERROR("Could not find a database with name %s", database_name.c_str());
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
    int column_idx = 0;
    auto &schema_columns = schema->GetColumns();
    for (auto &column : schema_columns) {
      if (column.IsPrimary()) {
        key_attrs.push_back(column_idx);
      }
      column_idx++;
    }

    key_schema = catalog::Schema::CopySchema(schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    bool unique_keys = true;

    index_metadata = new index::IndexMetadata(
        "customer_pkey", GetNextOid(), table->GetOid(), database->GetOid(),
        INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, schema,
        key_schema, key_attrs, unique_keys);

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

// Function to add non-primary Key index
Result Catalog::CreateIndex(const std::string &database_name,
                            const std::string &table_name,
                            std::vector<std::string> index_attr,
                            std::string index_name, bool unique,
                            IndexType index_type) {
  auto database = GetDatabaseWithName(database_name);
  if (database != nullptr) {
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
    auto &columns = schema->GetColumns();
    for (auto attr : index_attr) {
      for (uint i = 0; i < columns.size(); ++i) {
        if (attr == columns[i].column_name) {
          key_attrs.push_back(i);
        }
      }
    }

    // Check for mismatch between key attributes and attributes
    // that came out of the parser
    if (key_attrs.size() != index_attr.size()) {
      LOG_TRACE("Some columns are missing");
      return Result::RESULT_FAILURE;
    }

    key_schema = catalog::Schema::CopySchema(schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    // Check if unique index or not
    if (unique == false) {
      index_metadata = new index::IndexMetadata(
          index_name.c_str(), GetNextOid(), table->GetOid(), database->GetOid(),
          index_type, INDEX_CONSTRAINT_TYPE_DEFAULT, schema, key_schema,
          key_attrs, true);
    } else {
      index_metadata = new index::IndexMetadata(
          index_name.c_str(), GetNextOid(), table->GetOid(), database->GetOid(),
          index_type, INDEX_CONSTRAINT_TYPE_UNIQUE, schema, key_schema,
          key_attrs, true);
    }

    // Add index to table
    std::shared_ptr<index::Index> key_index(
        index::IndexFactory::GetInstance(index_metadata));
    table->AddIndex(key_index);

    LOG_TRACE("Successfully add index for table %s", table->GetName().c_str());
    return Result::RESULT_SUCCESS;
  }

  return Result::RESULT_FAILURE;
}

index::Index *Catalog::GetIndexWithOid(const oid_t database_oid,
                                       const oid_t table_oid,
                                       const oid_t index_oid) const {
  // Lookup table
  auto table = GetTableWithOid(database_oid, table_oid);

  // Lookup index
  if (table != nullptr) {
    auto index = table->GetIndexWithOid(index_oid);
    return index.get();
  }

  return nullptr;
}

// Drop a database
Result Catalog::DropDatabaseWithName(std::string database_name,
                                     concurrency::Transaction *txn) {
  LOG_TRACE("Dropping database %s", database_name.c_str());
  try {
    storage::Database *database = GetDatabaseWithName(database_name);

    LOG_TRACE("Found database!");
    LOG_TRACE("Deleting tuple from catalog");
    catalog::DeleteTuple(GetDatabaseWithName(CATALOG_DATABASE_NAME)
                             ->GetTableWithName(DATABASE_CATALOG_NAME),
                         database->GetOid(), txn);
    oid_t database_offset = 0;
    for (auto database : databases_) {
      if (database->GetDBName() == database_name) {
        LOG_TRACE("Deleting database object in database vector");
        delete database;
        break;
      }
      database_offset++;
    }
    PL_ASSERT(database_offset < databases_.size());
    // Drop the database
    LOG_TRACE("Deleting database from database vector");
    databases_.erase(databases_.begin() + database_offset);
  }
  catch (CatalogException &e) {
    LOG_TRACE("Database is not found!");
    return Result::RESULT_FAILURE;
  }
  return Result::RESULT_SUCCESS;
}

// Drop a database with its oid
void Catalog::DropDatabaseWithOid(const oid_t database_oid) {
  LOG_TRACE("Dropping database with oid: %d", database_oid);
  try {
    GetDatabaseWithOid(database_oid);
    LOG_TRACE("Found database!");
    LOG_TRACE("Deleting tuple from catalog");
    catalog::DeleteTuple(GetDatabaseWithName(CATALOG_DATABASE_NAME)
                             ->GetTableWithName(DATABASE_CATALOG_NAME),
                         database_oid, nullptr);
    oid_t database_offset = 0;
    for (auto database : databases_) {
      if (database->GetOid() == database_oid) {
        LOG_TRACE("Deleting database object in database vector");
        delete database;
        break;
      }
      database_offset++;
    }
    PL_ASSERT(database_offset < databases_.size());
    // Drop the database
    LOG_TRACE("Deleting database from database vector");
    databases_.erase(databases_.begin() + database_offset);
  }
  catch (CatalogException &e) {
    LOG_TRACE("Database is not found!");
  }
}

// Drop a table
Result Catalog::DropTable(std::string database_name, std::string table_name,
                          concurrency::Transaction *txn) {
  LOG_TRACE("Dropping table %s from database %s", table_name.c_str(),
            database_name.c_str());
  try {
    storage::Database *database = GetDatabaseWithName(database_name);
    LOG_TRACE("Found database!");
    storage::DataTable *table = database->GetTableWithName(table_name);

    if (table) {
      LOG_TRACE("Found table!");
      oid_t table_id = table->GetOid();
      LOG_TRACE("Deleting tuple from catalog!");
      catalog::DeleteTuple(GetDatabaseWithName(CATALOG_DATABASE_NAME)
                               ->GetTableWithName(TABLE_CATALOG_NAME),
                           table_id, txn);
      LOG_TRACE("Deleting table!");
      database->DropTableWithOid(table_id);
      return Result::RESULT_SUCCESS;
    } else {
      LOG_TRACE("Could not find table");
      return Result::RESULT_FAILURE;
    }
  }
  catch (CatalogException &e) {
    LOG_TRACE("Could not find database");
    return Result::RESULT_FAILURE;
  }
}

// Find a database using its id
storage::Database *Catalog::GetDatabaseWithOid(const oid_t db_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == db_oid) return database;
  throw CatalogException("Database with oid = " + std::to_string(db_oid) +
                         " is not found");
  return nullptr;
}

// Find a database using its name
storage::Database *Catalog::GetDatabaseWithName(const std::string database_name)
    const {
  for (auto database : databases_) {
    if (database != nullptr && database->GetDBName() == database_name)
      return database;
  }
  throw CatalogException("Database " + database_name + " is not found");
  return nullptr;
}

storage::Database *Catalog::GetDatabaseWithOffset(const oid_t database_offset)
    const {
  PL_ASSERT(database_offset < databases_.size());
  auto database = databases_.at(database_offset);
  return database;
}

// Get table from a database
storage::DataTable *Catalog::GetTableWithName(std::string database_name,
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

storage::DataTable *Catalog::GetTableWithOid(const oid_t database_oid,
                                             const oid_t table_oid) const {
  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
            table_oid);
  // Lookup DB
  auto database = GetDatabaseWithOid(database_oid);

  // Lookup table
  if (database != nullptr) {
    auto table = database->GetTableWithOid(table_oid);
    return table;
  }

  return nullptr;
}

// Create Table for pg_class
std::unique_ptr<storage::DataTable> Catalog::CreateTableCatalog(
    oid_t database_id, std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto table_schema = InitializeTablesSchema();

  catalog::Schema *schema = table_schema.release();
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      database_id, GetNextOid(), schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table));
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
      database_id, GetNextOid(), schema, database_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table));

  return table;
}

// Create table for metrics tables
std::unique_ptr<storage::DataTable> Catalog::CreateMetricsCatalog(
    oid_t database_id, std::string table_name) {
  bool own_schema = true;
  bool adapt_table = false;
  auto table_schema = InitializeQueryMetricsSchema();

  catalog::Schema *schema = nullptr;
  if (table_name == QUERY_METRIC_NAME) {
    schema = InitializeQueryMetricsSchema().release();
  } else if (table_name == TABLE_METRIC_NAME) {
    schema = InitializeTableMetricsSchema().release();
  } else if (table_name == DATABASE_METRIC_NAME) {
    schema = InitializeDatabaseMetricsSchema().release();
  } else if (table_name == INDEX_METRIC_NAME) {
    schema = InitializeIndexMetricsSchema().release();
  }

  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      database_id, GetNextOid(), schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table));

  return table;
}

// Initialize tables catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeTablesSchema() {
  const std::string not_null_constraint_name = "not_null";

  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "table_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto name_column =
      catalog::Column(common::Type::VARCHAR, max_name_size, "table_name", true);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      common::Type::VARCHAR, max_name_size, "database_name", true);
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
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "database_id", true);
  id_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));
  auto name_column = catalog::Column(common::Type::VARCHAR, max_name_size,
                                     "database_name", true);
  name_column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_schema(
      new catalog::Schema({id_column, name_column}));
  return database_schema;
}

// Initialize database catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeDatabaseMetricsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(CONSTRAINT_TYPE_NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = common::Type::GetTypeSize(common::Type::INTEGER);
  common::Type::TypeId integer_type = common::Type::INTEGER;

  auto id_column =
      catalog::Column(integer_type, integer_type_size, "database_id", true);
  id_column.AddConstraint(not_null_constraint);
  auto txn_committed_column =
      catalog::Column(integer_type, integer_type_size, "txn_committed", true);
  txn_committed_column.AddConstraint(not_null_constraint);
  auto txn_aborted_column =
      catalog::Column(integer_type, integer_type_size, "txn_aborted", true);
  txn_aborted_column.AddConstraint(not_null_constraint);

  auto timestamp_column =
      catalog::Column(integer_type, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> database_schema(new catalog::Schema(
      {id_column, txn_committed_column, txn_aborted_column, timestamp_column}));
  return database_schema;
}

// Initialize table catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeTableMetricsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(CONSTRAINT_TYPE_NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = common::Type::GetTypeSize(common::Type::INTEGER);
  common::Type::TypeId integer_type = common::Type::INTEGER;

  auto database_id_column =
      catalog::Column(integer_type, integer_type_size, "database_id", true);
  database_id_column.AddConstraint(not_null_constraint);
  auto table_id_column =
      catalog::Column(integer_type, integer_type_size, "table_id", true);
  table_id_column.AddConstraint(not_null_constraint);

  auto reads_column =
      catalog::Column(integer_type, integer_type_size, "reads", true);
  reads_column.AddConstraint(not_null_constraint);
  auto updates_column =
      catalog::Column(integer_type, integer_type_size, "updates", true);
  updates_column.AddConstraint(not_null_constraint);
  auto deletes_column =
      catalog::Column(integer_type, integer_type_size, "deletes", true);
  deletes_column.AddConstraint(not_null_constraint);
  auto inserts_column =
      catalog::Column(integer_type, integer_type_size, "inserts", true);
  inserts_column.AddConstraint(not_null_constraint);

  // MAX_INT only tracks the number of seconds since epoch until 2037
  auto timestamp_column =
      catalog::Column(integer_type, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> database_schema(new catalog::Schema(
      {database_id_column, table_id_column, reads_column,    updates_column,
       deletes_column,     inserts_column,  timestamp_column}));
  return database_schema;
}

// Initialize index catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeIndexMetricsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(CONSTRAINT_TYPE_NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = common::Type::GetTypeSize(common::Type::INTEGER);
  common::Type::TypeId integer_type = common::Type::INTEGER;

  auto database_id_column =
      catalog::Column(integer_type, integer_type_size, "database_id", true);
  database_id_column.AddConstraint(not_null_constraint);
  auto table_id_column =
      catalog::Column(integer_type, integer_type_size, "table_id", true);
  table_id_column.AddConstraint(not_null_constraint);
  auto index_id_column =
      catalog::Column(integer_type, integer_type_size, "index_id", true);
  index_id_column.AddConstraint(not_null_constraint);

  auto reads_column =
      catalog::Column(integer_type, integer_type_size, "reads", true);
  reads_column.AddConstraint(not_null_constraint);
  auto deletes_column =
      catalog::Column(integer_type, integer_type_size, "deletes", true);
  deletes_column.AddConstraint(not_null_constraint);
  auto inserts_column =
      catalog::Column(integer_type, integer_type_size, "inserts", true);
  inserts_column.AddConstraint(not_null_constraint);

  auto timestamp_column =
      catalog::Column(integer_type, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> database_schema(new catalog::Schema(
      {database_id_column, table_id_column, index_id_column, reads_column,
       deletes_column,     inserts_column,  timestamp_column}));
  return database_schema;
}

// Initialize query catalog schema
std::unique_ptr<catalog::Schema> Catalog::InitializeQueryMetricsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(CONSTRAINT_TYPE_NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = common::Type::GetTypeSize(common::Type::INTEGER);
  oid_t varbinary_type_size =
      common::Type::GetTypeSize(common::Type::VARBINARY);

  common::Type::TypeId integer_type = common::Type::INTEGER;
  common::Type::TypeId varbinary_type = common::Type::VARBINARY;

  auto name_column = catalog::Column(
      common::Type::VARCHAR, common::Type::GetTypeSize(common::Type::VARCHAR),
      "query_name", false);
  name_column.AddConstraint(not_null_constraint);
  auto database_id_column =
      catalog::Column(integer_type, integer_type_size, "database_id", true);
  database_id_column.AddConstraint(not_null_constraint);

  // Parameters
  auto num_param_column =
      catalog::Column(integer_type, integer_type_size, "num_params", true);
  num_param_column.AddConstraint(not_null_constraint);
  // For varbinary types, we don't want to inline it since it could be very
  // large
  auto param_format_column = catalog::Column(
      varbinary_type, varbinary_type_size, "param_formats", false);
  auto param_val_column = catalog::Column(varbinary_type, varbinary_type_size,
                                          "param_values", false);

  // Physical statistics
  auto reads_column =
      catalog::Column(integer_type, integer_type_size, "reads", true);
  reads_column.AddConstraint(not_null_constraint);
  auto updates_column =
      catalog::Column(integer_type, integer_type_size, "updates", true);
  updates_column.AddConstraint(not_null_constraint);
  auto deletes_column =
      catalog::Column(integer_type, integer_type_size, "deletes", true);
  deletes_column.AddConstraint(not_null_constraint);
  auto inserts_column =
      catalog::Column(integer_type, integer_type_size, "inserts", true);
  inserts_column.AddConstraint(not_null_constraint);
  auto latency_column =
      catalog::Column(integer_type, integer_type_size, "latency", true);
  latency_column.AddConstraint(not_null_constraint);
  auto cpu_time_column =
      catalog::Column(integer_type, integer_type_size, "cpu_time", true);

  // MAX_INT only tracks the number of seconds since epoch until 2037
  auto timestamp_column =
      catalog::Column(integer_type, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> database_schema(new catalog::Schema(
      {name_column,         database_id_column, num_param_column,
       param_format_column, param_val_column,   reads_column,
       updates_column,      deletes_column,     inserts_column,
       latency_column,      cpu_time_column,    timestamp_column}));
  return database_schema;
}

void Catalog::PrintCatalogs() {}

oid_t Catalog::GetDatabaseCount() { return databases_.size(); }

oid_t Catalog::GetNextOid() { return oid_++; }

Catalog::~Catalog() {
  delete GetDatabaseWithName(CATALOG_DATABASE_NAME);

  delete pool_;
}
}
}
