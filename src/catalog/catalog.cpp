//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/catalog/catalog.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"

#include "catalog/column_catalog.h"
#include "catalog/constraint_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "catalog/language_catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/proc_catalog.h"
#include "catalog/query_history_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/settings_catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/trigger_catalog.h"
#include "codegen/code_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "function/date_functions.h"
#include "function/numeric_functions.h"
#include "function/old_engine_string_functions.h"
#include "function/timestamp_functions.h"
#include "index/index_factory.h"
#include "settings/settings_manager.h"
#include "storage/storage_manager.h"
#include "storage/table_factory.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace catalog {

// Get instance of the global catalog
Catalog *Catalog::GetInstance() {
  static Catalog global_catalog;
  return &global_catalog;
}

/* Initialization of catalog, including:
 * 1) create peloton database, create catalog tables, add them into
 * peloton database, insert columns into pg_attribute
 * 2) create necessary indexes, insert into pg_index
 * 3) insert peloton into pg_database, catalog tables into pg_table
 */
Catalog::Catalog() : pool_(new type::EphemeralPool()) {
  // Begin transaction for catalog initialization
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto storage_manager = storage::StorageManager::GetInstance();
  // Create peloton database
  auto peloton = new storage::Database(CATALOG_DATABASE_OID);
  peloton->setDBName(CATALOG_DATABASE_NAME);
  storage_manager->AddDatabaseToStorageManager(peloton);

  // Create catalog tables
  DatabaseCatalog::GetInstance(txn, peloton, pool_.get());
  BootstrapSystemCatalogs(txn, peloton);

  // Insert peloton database into pg_database
  DatabaseCatalog::GetInstance(txn)->InsertDatabase(txn,
                                                    CATALOG_DATABASE_OID,
                                                    CATALOG_DATABASE_NAME,
                                                    pool_.get());

  // Commit transaction
  txn_manager.CommitTransaction(txn);
}

/*@brief   This function *MUST* be called after a new database is created to
 * bootstrap all system catalog tables for that database. The system catalog
 * tables must be created in certain order to make sure all tuples are indexed
 *
 * @param   database    database which this system catalogs belong to
 * @param   txn         transaction context
 */
void Catalog::BootstrapSystemCatalogs(concurrency::TransactionContext *txn,
                                      storage::Database *database) {
  oid_t database_oid = database->GetOid();
  catalog_map_.emplace(database_oid,
                       std::shared_ptr<SystemCatalogs>(
                           new SystemCatalogs(txn, database, pool_.get())));
  auto system_catalogs = catalog_map_[database_oid];

  // Create indexes on catalog tables, insert them into pg_index
  // actual index already added in
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  COLUMN_CATALOG_OID,
                                                  COLUMN_CATALOG_PKEY_OID,
                                                  COLUMN_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {ColumnCatalog::ColumnId::TABLE_OID,
                                                   ColumnCatalog::ColumnId::COLUMN_NAME},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  COLUMN_CATALOG_OID,
                                                  COLUMN_CATALOG_SKEY0_OID,
                                                  COLUMN_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::UNIQUE,
                                                  true,
                                                  {ColumnCatalog::ColumnId::TABLE_OID,
                                                   ColumnCatalog::ColumnId::COLUMN_ID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  COLUMN_CATALOG_OID,
                                                  COLUMN_CATALOG_SKEY1_OID,
                                                  COLUMN_CATALOG_NAME "_skey1",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::DEFAULT,
                                                  false,
                                                  {ColumnCatalog::ColumnId::TABLE_OID},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  INDEX_CATALOG_OID,
                                                  INDEX_CATALOG_PKEY_OID,
                                                  INDEX_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {IndexCatalog::ColumnId::INDEX_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  INDEX_CATALOG_OID,
                                                  INDEX_CATALOG_SKEY0_OID,
                                                  INDEX_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::UNIQUE,
                                                  true,
                                                  {IndexCatalog::ColumnId::INDEX_NAME},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  INDEX_CATALOG_OID,
                                                  INDEX_CATALOG_SKEY1_OID,
                                                  INDEX_CATALOG_NAME "_skey1",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::DEFAULT,
                                                  false,
                                                  {IndexCatalog::ColumnId::TABLE_OID},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  DATABASE_CATALOG_OID,
                                                  DATABASE_CATALOG_PKEY_OID,
                                                  DATABASE_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {DatabaseCatalog::ColumnId::DATABASE_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  DATABASE_CATALOG_OID,
                                                  DATABASE_CATALOG_SKEY0_OID,
                                                  DATABASE_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::UNIQUE,
                                                  true,
                                                  {DatabaseCatalog::ColumnId::DATABASE_NAME},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  SCHEMA_CATALOG_OID,
                                                  SCHEMA_CATALOG_PKEY_OID,
                                                  SCHEMA_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {SchemaCatalog::ColumnId::SCHEMA_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  SCHEMA_CATALOG_OID,
                                                  SCHEMA_CATALOG_SKEY0_OID,
                                                  SCHEMA_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::UNIQUE,
                                                  true,
                                                  {SchemaCatalog::ColumnId::SCHEMA_NAME},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  TABLE_CATALOG_OID,
                                                  TABLE_CATALOG_PKEY_OID,
                                                  TABLE_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {TableCatalog::ColumnId::TABLE_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  TABLE_CATALOG_OID,
                                                  TABLE_CATALOG_SKEY0_OID,
                                                  TABLE_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::UNIQUE,
                                                  true,
                                                  {TableCatalog::ColumnId::TABLE_NAME},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  TABLE_CATALOG_OID,
                                                  TABLE_CATALOG_SKEY1_OID,
                                                  TABLE_CATALOG_NAME "_skey1",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::DEFAULT,
                                                  false,
                                                  {TableCatalog::ColumnId::DATABASE_OID},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  LAYOUT_CATALOG_OID,
                                                  LAYOUT_CATALOG_PKEY_OID,
                                                  LAYOUT_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {LayoutCatalog::ColumnId::TABLE_OID,
                                                   LayoutCatalog::ColumnId::LAYOUT_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  LAYOUT_CATALOG_OID,
                                                  LAYOUT_CATALOG_SKEY0_OID,
                                                  LAYOUT_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::DEFAULT,
                                                  true,
                                                  {LayoutCatalog::ColumnId::TABLE_OID},
                                                  pool_.get());

  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  CONSTRAINT_CATALOG_OID,
                                                  CONSTRAINT_CATALOG_PKEY_OID,
                                                  CONSTRAINT_CATALOG_NAME "_pkey",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::PRIMARY_KEY,
                                                  true,
                                                  {ConstraintCatalog::ColumnId::CONSTRAINT_OID},
                                                  pool_.get());
  system_catalogs->GetIndexCatalog()->InsertIndex(txn,
                                                  CATALOG_SCHEMA_NAME,
                                                  CONSTRAINT_CATALOG_OID,
                                                  CONSTRAINT_CATALOG_SKEY0_OID,
                                                  CONSTRAINT_CATALOG_NAME "_skey0",
                                                  IndexType::BWTREE,
                                                  IndexConstraintType::DEFAULT,
                                                  true,
                                                  {ConstraintCatalog::ColumnId::TABLE_OID},
                                                  pool_.get());

  // Insert records(default + pg_catalog namespace) into pg_namespace
  system_catalogs->GetSchemaCatalog()->InsertSchema(txn,
                                                    CATALOG_SCHEMA_OID,
                                                    CATALOG_SCHEMA_NAME,
                                                    pool_.get());
  system_catalogs->GetSchemaCatalog()->InsertSchema(txn,
                                                    DEFAULT_SCHEMA_OID,
                                                    DEFAULT_SCHEMA_NAME,
                                                    pool_.get());

  // Insert catalog tables into pg_table
  // pg_database record is shared across different databases
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  CATALOG_DATABASE_OID,
                                                  CATALOG_SCHEMA_NAME,
                                                  DATABASE_CATALOG_OID,
                                                  DATABASE_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  SCHEMA_CATALOG_OID,
                                                  SCHEMA_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  TABLE_CATALOG_OID,
                                                  TABLE_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  INDEX_CATALOG_OID,
                                                  INDEX_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  COLUMN_CATALOG_OID,
                                                  COLUMN_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  LAYOUT_CATALOG_OID,
                                                  LAYOUT_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
  system_catalogs->GetTableCatalog()->InsertTable(txn,
                                                  database_oid,
                                                  CATALOG_SCHEMA_NAME,
                                                  CONSTRAINT_CATALOG_OID,
                                                  CONSTRAINT_CATALOG_NAME,
                                                  ROW_STORE_LAYOUT_OID,
                                                  pool_.get());
}

void Catalog::Bootstrap() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // bootstrap pg_catalog database
  catalog_map_[CATALOG_DATABASE_OID]->Bootstrap(txn, CATALOG_DATABASE_NAME);
  // bootstrap other global catalog tables
  DatabaseMetricsCatalog::GetInstance(txn);
  SettingsCatalog::GetInstance(txn);
  LanguageCatalog::GetInstance(txn);

  // TODO: change pg_proc to per database
  ProcCatalog::GetInstance(txn);

  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    QueryHistoryCatalog::GetInstance(txn);
  }

  txn_manager.CommitTransaction(txn);

  InitializeLanguages();
  InitializeFunctions();

  // Reset oid of each catalog to avoid collisions between catalog
  // values added by system and users when checkpoint recovery.
  DatabaseCatalog::GetInstance(nullptr,
                               nullptr,
                               nullptr)->UpdateOid(OID_FOR_USER_OFFSET);
  LanguageCatalog::GetInstance().UpdateOid(OID_FOR_USER_OFFSET);
  ProcCatalog::GetInstance().UpdateOid(OID_FOR_USER_OFFSET);
}

//===----------------------------------------------------------------------===//
// CREATE FUNCTIONS
//===----------------------------------------------------------------------===//

ResultType Catalog::CreateDatabase(concurrency::TransactionContext *txn,
                                   const std::string &database_name) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create database " +
        database_name);

  auto pg_database = DatabaseCatalog::GetInstance(nullptr, nullptr, nullptr);
  auto storage_manager = storage::StorageManager::GetInstance();
  // Check if a database with the same name exists
  auto database_object =
      pg_database->GetDatabaseCatalogEntry(txn, database_name);
  if (database_object != nullptr)
    throw CatalogException("Database " + database_name + " already exists");

  // Create actual database
  oid_t database_oid = pg_database->GetNextOid();

  storage::Database *database = new storage::Database(database_oid);

  // TODO: This should be deprecated, dbname should only exists in pg_db
  database->setDBName(database_name);
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    storage_manager->AddDatabaseToStorageManager(database);
  }
  // put database object into rw_object_set
  txn->RecordCreate(database_oid, INVALID_OID, INVALID_OID);
  // Insert database record into pg_db
  pg_database->InsertDatabase(txn, database_oid, database_name, pool_.get());

  // add core & non-core system catalog tables into database
  BootstrapSystemCatalogs(txn, database);
  catalog_map_[database_oid]->Bootstrap(txn, database_name);
  LOG_TRACE("Database %s created. Returning RESULT_SUCCESS.",
            database_name.c_str());
  return ResultType::SUCCESS;
}

/*@brief   create schema(namespace)
 * @param   database_name    the database which the namespace belongs to
 * @param   schema_name      name of the schema
 * @param   txn              TransactionContext
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateSchema(concurrency::TransactionContext *txn,
                                 const std::string &database_name,
                                 const std::string &schema_name) {
  if (txn == nullptr)
    throw CatalogException(
        "Do not have transaction to create schema(namespace) " + database_name);

  // check whether database exists from pg_database
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);
  if (database_object == nullptr)
    throw CatalogException("Can't find Database " + database_name +
        " to create schema");
  // check whether namespace exists from pg_namespace
  auto pg_namespace =
      catalog_map_[database_object->GetDatabaseOid()]->GetSchemaCatalog();
  auto schema_object = pg_namespace->GetSchemaCatalogEntry(txn, schema_name);
  if (schema_object != nullptr)
    throw CatalogException("Schema(namespace) " + schema_name +
        " already exists");
  // Since there isn't physical class corresponds to schema(namespace), the only
  // thing needs to be done is inserting record into pg_namespace
  pg_namespace->InsertSchema(txn,
                             pg_namespace->GetNextOid(),
                             schema_name,
                             pool_.get());

  LOG_TRACE("Schema(namespace) %s created. Returning RESULT_SUCCESS.",
            schema_name.c_str());
  return ResultType::SUCCESS;
}

/*@brief   create table
 * @param   txn              TransactionContext
 * @param   database_name    the database which the table belongs to
 * @param   schema_name      name of schema the table belongs to
 * @param   schema           schema, a.k.a metadata of the table
 * @param   table_name       name of the table
 * @param   is_catalog       table is built as catalog or not(useful in
 *                           catalog table Initialization)
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateTable(concurrency::TransactionContext *txn,
                                const std::string &database_name,
                                const std::string &schema_name,
                                std::unique_ptr<catalog::Schema> schema,
                                const std::string &table_name,
                                bool is_catalog,
                                uint32_t tuples_per_tilegroup,
                                LayoutType layout_type) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create table " +
        table_name);

  LOG_TRACE("Creating table %s in database %s", table_name.c_str(),
            database_name.c_str());
  // check whether database exists from pg_database
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);
  if (database_object == nullptr)
    throw CatalogException("Can't find Database " + database_name +
        " to create table");
  // check whether namespace exists from pg_namespace
  auto schema_object = catalog_map_[database_object->GetDatabaseOid()]
      ->GetSchemaCatalog()
      ->GetSchemaCatalogEntry(txn, schema_name);
  if (schema_object == nullptr)
    throw CatalogException("Can't find namespace " + schema_name +
        " to create table");

  // get table oid from pg_table
  auto table_object =
      database_object->GetTableCatalogEntry(table_name, schema_name);
  if (table_object != nullptr)
    throw CatalogException("Table: " + schema_name + "." + table_name +
        " already exists");

  auto storage_manager = storage::StorageManager::GetInstance();
  auto database =
      storage_manager->GetDatabaseWithOid(database_object->GetDatabaseOid());

  // Check duplicate column names
  std::set<std::string> column_names;
  auto columns = schema.get()->GetColumns();

  for (auto column : columns) {
    auto column_name = column.GetName();
    if (column_names.count(column_name) == 1)
      throw CatalogException("Can't create table " + table_name +
          " with duplicate column name");
    column_names.insert(column_name);
  }

  // Create actual table
  auto pg_table =
      catalog_map_[database_object->GetDatabaseOid()]->GetTableCatalog();
  auto pg_attribute =
      catalog_map_[database_object->GetDatabaseOid()]->GetColumnCatalog();
  oid_t table_oid = pg_table->GetNextOid();
  bool own_schema = true;
  bool adapt_table = false;
  auto table = storage::TableFactory::GetDataTable(
      database_object->GetDatabaseOid(), table_oid, schema.release(),
      table_name, tuples_per_tilegroup, own_schema, adapt_table, is_catalog,
      layout_type);
  database->AddTable(table, is_catalog);
  // put data table object into rw_object_set
  txn->RecordCreate(database_object->GetDatabaseOid(), table_oid, INVALID_OID);

  // Update pg_table with table info
  pg_table->InsertTable(txn,
                        database_object->GetDatabaseOid(),
                        schema_name,
                        table_oid,
                        table_name,
                        table->GetDefaultLayout()->GetOid(),
                        pool_.get());

  // Insert column info into each catalog
  oid_t column_id = 0;
  for (const auto &column : table->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(txn,
                               table_oid,
                               column.GetName(),
                               column_id,
                               column.GetOffset(),
                               column.GetType(),
                               column.GetLength(),
                               column.IsInlined(),
                               column.IsNotNull(),
                               column.HasDefault(),
                               column.GetDefaultValue(),
                               pool_.get());
    column_id++;
  }

  // Create layout as default layout
  auto pg_layout =
      catalog_map_[database_object->GetDatabaseOid()]->GetLayoutCatalog();
  auto default_layout = table->GetDefaultLayout();
  if (!pg_layout->InsertLayout(txn, table_oid, default_layout, pool_.get()))
    throw CatalogException("Failed to create a new layout for table "
                               + table_name);

  return ResultType::SUCCESS;
}

/*@brief   create index on table
 * @param   txn              TransactionContext
 * @param   database_name    the database which the indexed table belongs to
 * @param   schema_name      the namespace which the indexed table belongs to
 * @param   table_name       name of the table to add index on
 * @param   index_name       name of the table to add index on
 * @param   key_attrs        collection of the indexed attribute(column) name
 * @param   unique_keys      index supports duplicate key or not
 * @param   index_type       the type of index(default value is BWTREE)
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateIndex(concurrency::TransactionContext *txn,
                                const std::string &database_name,
                                const std::string &schema_name,
                                const std::string &table_name,
                                const std::string &index_name,
                                const std::vector<oid_t> &key_attrs,
                                bool unique_keys,
                                IndexType index_type) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create database " +
        index_name);

  LOG_TRACE("Trying to create index %s for table %s", index_name.c_str(),
            table_name.c_str());

  // check if database exists
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);
  if (database_object == nullptr)
    throw CatalogException("Can't find Database " + database_name +
        " to create index");

  // check if table exists
  auto table_object =
      database_object->GetTableCatalogEntry(table_name, schema_name);
  if (table_object == nullptr)
    throw CatalogException("Can't find table " + schema_name + "." +
        table_name + " to create index");

  auto pg_index =
      catalog_map_[database_object->GetDatabaseOid()]->GetIndexCatalog();
  oid_t index_oid = pg_index->GetNextOid();
  IndexConstraintType index_constraint =
      unique_keys ? IndexConstraintType::UNIQUE : IndexConstraintType::DEFAULT;

  ResultType success = CreateIndex(txn,
                                   database_object->GetDatabaseOid(),
                                   schema_name,
                                   table_object->GetTableOid(),
                                   false,
                                   index_oid,
                                   index_name,
                                   key_attrs,
                                   unique_keys,
                                   index_type,
                                   index_constraint);

  return success;
}

/*@brief   create index on table
 * @param   txn              TransactionContext
 * @param   database_oid     the database which the indexed table belongs to
 * @param   schema_name      the namespace which the indexed table belongs to
 * @param   table_oid        name of the table to add index on
 * @param   is_catalog       index is built on catalog table or not(useful in
 *                           catalog table Initialization)
 * @param   index_oid        oid of the index to be added
 * @param   index_name       index name to be added
 * @param   key_attrs        collection of the indexed attribute(column) name
 * @param   unique_keys      index supports duplicate key or not
 * @param   index_type       the type of index
 * @param   index_constraint the constraint type of index
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateIndex(concurrency::TransactionContext *txn,
                                oid_t database_oid,
                                const std::string &schema_name,
                                oid_t table_oid,
                                bool is_catalog,
                                oid_t index_oid,
                                const std::string &index_name,
                                const std::vector<oid_t> &key_attrs,
                                bool unique_keys,
                                IndexType index_type,
                                IndexConstraintType index_constraint) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create index " +
        index_name);

  LOG_TRACE("Trying to create index for table %d", table_oid);

  if (is_catalog == false) {
    // check if table already has index with same name
    // only check when is_catalog flag == false
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid);
    auto table_object = database_object->GetTableCatalogEntry(table_oid);
    auto index_object = table_object->GetIndexCatalogEntry(index_name);

    if (index_object != nullptr)
      throw CatalogException("Index " + index_name + " already exists in" +
          database_object->GetDatabaseName());
  }
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto table = database->GetTableWithOid(table_oid);
  auto schema = table->GetSchema();

  // Passed all checks, now get all index metadata
  LOG_TRACE("Trying to create index %s on table %d", index_name.c_str(),
            table_oid);
  auto key_schema = catalog::Schema::CopySchema(schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  // Set index metadata
  auto index_metadata = new index::IndexMetadata(
      index_name, index_oid, table_oid, database_oid, index_type,
      index_constraint, schema, key_schema, key_attrs, unique_keys);

  // Add index to table
  std::shared_ptr<index::Index> key_index(
      index::IndexFactory::GetIndex(index_metadata));
  table->AddIndex(key_index);

  // Put index object into rw_object_set
  txn->RecordCreate(database_oid, table_oid, index_oid);
  // Insert index record into pg_index
  auto pg_index = catalog_map_[database_oid]->GetIndexCatalog();
  pg_index->InsertIndex(txn,
                        schema_name,
                        table_oid,
                        index_oid,
                        index_name,
                        index_type,
                        index_constraint,
                        unique_keys,
                        key_attrs,
                        pool_.get());

  LOG_TRACE("Successfully add index for table %s contains %d indexes",
            table->GetName().c_str(), (int) table->GetValidIndexCount());
  return ResultType::SUCCESS;
}

std::shared_ptr<const storage::Layout> Catalog::CreateLayout(concurrency::TransactionContext *txn,
                                                             oid_t database_oid,
                                                             oid_t table_oid,
                                                             const column_map_type &column_map) {
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto table = database->GetTableWithOid(table_oid);

  oid_t layout_oid = table->GetNextLayoutOid();
  // Ensure that the new layout
  PELOTON_ASSERT(layout_oid < INVALID_OID);
  auto new_layout = std::shared_ptr<const storage::Layout>(
      new const storage::Layout(column_map, column_map.size(), layout_oid));

  // Add the layout the pg_layout table
  auto pg_layout = catalog_map_[database_oid]->GetLayoutCatalog();
  if (pg_layout->GetLayoutWithOid(txn, table_oid, new_layout->GetOid())
      == nullptr &&
      !pg_layout->InsertLayout(txn, table_oid, new_layout, pool_.get())) {
    LOG_ERROR("Failed to create a new layout for table %u", table_oid);
    return nullptr;
  }
  return new_layout;
}

std::shared_ptr<const storage::Layout> Catalog::CreateDefaultLayout(concurrency::TransactionContext *txn,
                                                                    oid_t database_oid,
                                                                    oid_t table_oid,
                                                                    const column_map_type &column_map) {
  auto new_layout = CreateLayout(txn, database_oid, table_oid, column_map);
  // If the layout creation was successful, set it as the default
  if (new_layout != nullptr) {
    auto storage_manager = storage::StorageManager::GetInstance();
    auto database = storage_manager->GetDatabaseWithOid(database_oid);
    auto table = database->GetTableWithOid(table_oid);
    table->SetDefaultLayout(new_layout);

    // update table catalog
    catalog_map_[database_oid]->GetTableCatalog()
        ->UpdateDefaultLayoutOid(txn, table_oid, new_layout->GetOid());
  }
  return new_layout;
}

//===--------------------------------------------------------------------===//
// SET FUNCTIONS FOR COLUMN CONSTRAINT
//===--------------------------------------------------------------------===//

/**
 * @brief   Set not null constraint for a column
 * @param   txn           TransactionContext
 * @param   database_oid  Database to which the table belongs to
 * @param   table_oid     Table to which the constraint has to be set
 * @param   column_id     Column that the constraint affects
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::SetNotNullConstraint(concurrency::TransactionContext *txn,
                                         oid_t database_oid,
                                         oid_t table_oid,
                                         oid_t column_id) {
  auto table_object = catalog_map_[database_oid]
                                   ->GetTableCatalog()
                                   ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();
  auto column = schema->GetColumn(column_id);

  // Check not null
  if (column.IsNotNull()) {
    throw CatalogException("Column " + column.GetName() + " in table " +
                           table_object->GetTableName() +
                           " is already NOT NULL.");
  }

  // Update pg_column to set constraint of the column
  auto pg_column = catalog_map_[database_oid]->GetColumnCatalog();
  pg_column->UpdateNotNullConstraint(txn,
                                     table_oid,
                                     column.GetName(),
                                     true);

  // Set not null constraint in the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->SetNotNull(column_id);
  }

  LOG_TRACE("Added a NOT NULL constraint to column %s in %s.",
            column.GetName().c_str(), table_object->GetTableName().c_str());

  return ResultType::SUCCESS;
}

/**
 * @brief   Set default constraint for a column
 * @param   txn           TransactionContext
 * @param   database_oid  Database to which the table belongs to
 * @param   table_oid     Table to which the constraint has to be set
 * @param   column_id     Column that the constraint affects
 * @param   default_value Value for the default constraint
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::SetDefaultConstraint(concurrency::TransactionContext *txn,
                                         oid_t database_oid,
                                         oid_t table_oid,
                                         oid_t column_id,
                                         const type::Value &default_value) {
  auto table_object = catalog_map_[database_oid]->GetTableCatalog()
                                                ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();
  auto column = schema->GetColumn(column_id);

  // Check default
  if (column.HasDefault()) {
    throw CatalogException("Column " + column.GetName() + " in table" +
                           table_object->GetTableName() +
                           " is already set default value '" +
                           column.GetDefaultValue()->ToString() + "'.");
  }

  // Update pg_column to set constraint of the column
  auto pg_column = catalog_map_[database_oid]->GetColumnCatalog();
  pg_column->UpdateDefaultConstraint(txn,
                                     table_oid,
                                     column.GetName(),
                                     true,
                                     &default_value);

  // Set default constraint in the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->SetDefaultValue(column_id, default_value);
  }

  LOG_TRACE("Added a DEFAULT constraint to column %s in %s.",
            column.GetName().c_str(), table_object->GetTableName().c_str());

  return ResultType::SUCCESS;
}

//===--------------------------------------------------------------------===//
// ADD FUNCTIONS FOR TABLE CONSTRAINT
//===--------------------------------------------------------------------===//

/**
 * @brief   Add a new primary constraint for a table
 * @param   txn           TransactionContext
 * @param   database_oid  Database to which the table belongs to
 * @param   table_oid     Table to which the constraint has to be added
 * @param   column_ids    Columns that the constraint affects
 * @param   constraint_name  constraint name
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::AddPrimaryKeyConstraint(concurrency::TransactionContext *txn,
                                            oid_t database_oid,
                                            oid_t table_oid,
                                            const std::vector<oid_t> &column_ids,
                                            const std::string &constraint_name) {
  auto table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn,table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();

  // Check primary key in the table
  if (schema->HasPrimary()) {
    throw CatalogException("Table " + table_object->GetTableName() +
                           " already has primary key.");
  }

  // Create index
  std::string index_name = table_object->GetTableName() + "_pkey";
  auto index_oid = catalog_map_[database_oid]->GetIndexCatalog()->GetNextOid();
  CreateIndex(txn,
              database_oid,
              table_object->GetSchemaName(),
              table_oid,
              false,
              index_oid,
              index_name,
              column_ids,
              true,
              IndexType::BWTREE,
              IndexConstraintType::PRIMARY_KEY);

  // Insert constraint into pg_constraint
  auto pg_constraint = catalog_map_[database_oid]->GetConstraintCatalog();
  std::shared_ptr<Constraint> constraint(
      new Constraint(pg_constraint->GetNextOid(),
                     ConstraintType::PRIMARY,
                     constraint_name,
                     table_oid,
                     column_ids,
                     index_oid));
  pg_constraint->InsertConstraint(txn, constraint, pool_.get());

  // Add constraint into the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->AddConstraint(constraint);
  }

  LOG_TRACE("Added a PRIMARY KEY constraint in %s.",
            table_object->GetTableName().c_str());

  return ResultType::SUCCESS;
}

/**
 * @brief   Add a new unique constraint for a table
 * @param   txn           TransactionContext
 * @param   database_oid  Database to which the table belongs to
 * @param   table_oid     Table to which the constraint has to be added
 * @param   column_ids    Columns that the constraint affects
 * @param   constraint_name  constraint name
 * @return  ResultType(SUCCESS or FAILURE)
 * note: if add a new foreign key constraint, use AddForeignKeyConstraint
 */
ResultType Catalog::AddUniqueConstraint(concurrency::TransactionContext *txn,
                                        oid_t database_oid,
                                        oid_t table_oid,
                                        const std::vector<oid_t> &column_ids,
                                        const std::string &constraint_name) {
  auto table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();

  // Create index
  std::stringstream index_name(table_object->GetTableName());
  for (auto column_id : column_ids)
    index_name << "_" + schema->GetColumn(column_id).GetName();
  index_name << "_UNIQ";
  auto index_oid = catalog_map_[database_oid]->GetIndexCatalog()->GetNextOid();
  CreateIndex(txn,
              database_oid,
              table_object->GetSchemaName(),
              table_oid,
              false,
              index_oid,
              index_name.str(),
              column_ids,
              true,
              IndexType::BWTREE,
              IndexConstraintType::UNIQUE);

  // Insert constraint into pg_constraint
  auto pg_constraint = catalog_map_[database_oid]->GetConstraintCatalog();
  std::shared_ptr<Constraint> constraint(
      new Constraint(pg_constraint->GetNextOid(),
                     ConstraintType::UNIQUE,
                     constraint_name,
                     table_oid,
                     column_ids,
                     index_oid));
  pg_constraint->InsertConstraint(txn, constraint, pool_.get());

  // Add constraint into the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->AddConstraint(constraint);
  }

  LOG_TRACE("Added a UNIQUE constraint in %s.",
            table_object->GetTableName().c_str());

  return ResultType::SUCCESS;
}

/**
 * @brief   Add a new foreign key constraint for a table
 * @param   txn           TransactionContext
 * @param   database_oid  database to which the table belongs to
 * @param   src_table_oid table to which the constraint has to be added
 * @param   src_col_ids   Columns that the constraint affects
 * @param   sink_table_oid  sink table
 * @param   sink_col_ids  Columns that limit the source columns
 * @param   upd_action    foreign key constraint action when update
 * @param   del_action    foreign key constraint action when delete
 * @param   constraint_name  constraint name
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::AddForeignKeyConstraint(concurrency::TransactionContext *txn,
                                            oid_t database_oid, oid_t src_table_oid,
                                            const std::vector<oid_t> &src_col_ids,
                                            oid_t sink_table_oid,
                                            const std::vector<oid_t> &sink_col_ids,
                                            FKConstrActionType upd_action,
                                            FKConstrActionType del_action,
                                            const std::string &constraint_name) {
  auto pg_constraint = catalog_map_[database_oid]->GetConstraintCatalog();
  auto constraint_oid = pg_constraint->GetNextOid();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto src_table =
      storage_manager->GetTableWithOid(database_oid, src_table_oid);
  auto sink_table =
      storage_manager->GetTableWithOid(database_oid, sink_table_oid);

  // Add a non-unique index on the source table if needed
  auto src_table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn, src_table_oid);
  auto src_schema = src_table->GetSchema();

  std::stringstream index_name(src_table_object->GetTableName());
  for (auto col_id : src_col_ids)
    index_name << "_" << src_schema->GetColumn(col_id).GetName();
  index_name << "_fkey";
  oid_t index_oid = catalog_map_[database_oid]->GetIndexCatalog()->GetNextOid();
  CreateIndex(txn,
              database_oid,
              src_table_object->GetSchemaName(),
              src_table_oid,
              false,
              index_oid,
              index_name.str(),
              src_col_ids,
              false,
              IndexType::BWTREE,
              IndexConstraintType::DEFAULT);

  // Insert constraint into pg_constraint
  std::shared_ptr<Constraint> constraint(
      new Constraint(constraint_oid,
                     ConstraintType::FOREIGN,
                     constraint_name,
                     src_table_oid,
                     src_col_ids,
                     index_oid,
                     sink_table_oid,
                     sink_col_ids,
                     upd_action,
                     del_action));
  pg_constraint->InsertConstraint(txn, constraint, pool_.get());

  // add constraint into schema in source table and sink table
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    src_schema->AddConstraint(constraint);
    sink_table->GetSchema()->RegisterForeignKeySource(constraint);
  }

  LOG_TRACE("Added a FOREIGN KEY constraint in %s to %s.",
            src_table_object->GetTableName().c_str(),
            catalog_map_[database_oid]
                ->GetTableCatalog()
                ->GetTableCatalogEntry(txn, sink_table_oid)
                ->GetTableName()
                .c_str());

  return ResultType::SUCCESS;
}

/**
 * @brief   Add a new check constraint for a table
 * @param   txn           TransactionContext
 * @param   database_oid  Database to which the table belongs to
 * @param   table_oid     Table to which the constraint has to be added
 * @param   column_ids    Columns that the constraint affects
 * @param   cmd           Command string for check constraint
 * @param   exp           Expression for check constraint
 * @param   constraint_name  constraint name
 * @return  ResultType(SUCCESS or FAILURE)
 * note: if add a new foreign key constraint, use AddForeignKeyConstraint
 */
ResultType Catalog::AddCheckConstraint(concurrency::TransactionContext *txn,
                                       oid_t database_oid,
                                       oid_t table_oid,
                                       const std::vector<oid_t> &column_ids,
                                       const std::pair<ExpressionType,
                                       type::Value> &exp,
                                       const std::string &constraint_name) {
  auto table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();

  // Insert constraint into pg_constraint
  auto pg_constraint = catalog_map_[database_oid]->GetConstraintCatalog();
  std::shared_ptr<Constraint> constraint(
      new Constraint(pg_constraint->GetNextOid(),
                     ConstraintType::CHECK,
                     constraint_name,
                     table_oid,
                     column_ids,
                     INVALID_OID,
                     exp));
  pg_constraint->InsertConstraint(txn, constraint, pool_.get());

  // Add constraint into the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->AddConstraint(constraint);
  }

  LOG_TRACE("Added a CHECK constraint in %s.",
            table_object->GetTableName().c_str());

  return ResultType::SUCCESS;
}

//===----------------------------------------------------------------------===//
// DROP FUNCTIONS
//===----------------------------------------------------------------------===//

ResultType Catalog::DropDatabaseWithName(concurrency::TransactionContext *txn,
                                         const std::string &database_name) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop database " +
        database_name);

  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);
  if (database_object == nullptr)
    throw CatalogException("Drop Database: " + database_name +
        " does not exist");

  return DropDatabaseWithOid(txn, database_object->GetDatabaseOid());
}

ResultType Catalog::DropDatabaseWithOid(concurrency::TransactionContext *txn,
                                        oid_t database_oid) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop database " +
        std::to_string(database_oid));
  auto storage_manager = storage::StorageManager::GetInstance();
  // Drop actual tables in the database
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_oid);
  auto table_objects = database_object->GetTableCatalogEntries();
  for (auto it : table_objects) {
    DropTable(txn, database_oid, it.second->GetTableOid());
  }

  // Drop database record in catalog
  if (!DatabaseCatalog::GetInstance(nullptr,
                                    nullptr,
                                    nullptr)->DeleteDatabase(txn, database_oid))
    throw CatalogException("Database record: " + std::to_string(database_oid) +
        " does not exist in pg_database");

  catalog_map_.erase(database_oid);
  // put database object into rw_object_set
  storage_manager->GetDatabaseWithOid(database_oid);
  txn->RecordDrop(database_oid, INVALID_OID, INVALID_OID);

  return ResultType::SUCCESS;
}

/*@brief   Drop schema
 * 1. drop all the tables within this schema
 * 2. delete record within pg_namespace
 * @param   database_name    the database which the dropped table belongs to
 * @param   schema_name      the dropped schema(namespace) name
 * @param   txn              TransactionContext
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropSchema(concurrency::TransactionContext *txn,
                               const std::string &database_name,
                               const std::string &schema_name) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop schema " +
        schema_name);

  auto database_object =
      DatabaseCatalog::GetInstance(txn)->GetDatabaseCatalogEntry(txn,
                                                                 database_name);
  if (database_object == nullptr)
    throw CatalogException("Drop Schema: database " + database_name +
        " does not exist");

  // check whether namespace exists from pg_namespace
  auto pg_namespace =
      catalog_map_[database_object->GetDatabaseOid()]->GetSchemaCatalog();
  auto schema_object = pg_namespace->GetSchemaCatalogEntry(txn, schema_name);
  if (schema_object == nullptr)
    throw CatalogException("Can't find namespace " + schema_name + " to drop");

  auto table_objects = database_object->GetTableCatalogEntries(schema_name);
  for (auto it : table_objects) {
    DropTable(txn, it->GetDatabaseOid(), it->GetTableOid());
  }

  // remove record within pg_namespace
  pg_namespace->DeleteSchema(txn, schema_name);
  return ResultType::SUCCESS;
}

/*@brief   Drop table
 * 1. drop all the indexes on actual table, and drop index records in
 * pg_index
 * 2. drop all the columns records in pg_attribute
 * 3. drop table record in pg_table
 * 4. delete actual table(storage level), cleanup schema, foreign keys,
 * tile_groups
 * @param   database_name    the database which the dropped table belongs to
 * @param   schema_name      the namespace which the dropped table belongs
 * to
 * @param   table_name       the dropped table name
 * @param   txn              TransactionContext
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropTable(concurrency::TransactionContext *txn,
                              const std::string &database_name,
                              const std::string &schema_name,
                              const std::string &table_name) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop table " +
        table_name);

  // Checking if statement is valid
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);
  if (database_object == nullptr)
    throw CatalogException("Drop Table: database " + database_name +
        " does not exist");

  // check if table exists
  auto table_object =
      database_object->GetTableCatalogEntry(table_name, schema_name);
  if (table_object == nullptr)
    throw CatalogException("Drop Table: table " + schema_name + "." +
        table_name + " does not exist");

  ResultType result = DropTable(txn,
                                database_object->GetDatabaseOid(),
                                table_object->GetTableOid());
  return result;
}

/*@brief   Drop table
 * 1. drop all the indexes on actual table, and drop index records in pg_index
 * 2. drop all the columns records in pg_attribute
 * 3. drop table record in pg_table
 * 4. delete actual table(storage level), cleanup schema, foreign keys,
 * tile_groups
 * @param   database_oid    the database which the dropped table belongs to
 * @param   table_oid       the dropped table name
 * @param   txn             TransactionContext
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropTable(concurrency::TransactionContext *txn,
                              oid_t database_oid,
                              oid_t table_oid) {
  LOG_TRACE("Dropping table %d from database %d", database_oid, table_oid);
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_oid);
  auto table_object = database_object->GetTableCatalogEntry(table_oid);
  auto index_objects = table_object->GetIndexCatalogEntries();
  LOG_TRACE("dropping #%d indexes", (int) index_objects.size());
  // delete trigger and records in pg_trigger
  auto pg_trigger =
      catalog_map_[database_object->GetDatabaseOid()]->GetTriggerCatalog();
  std::unique_ptr<trigger::TriggerList> trigger_lists =
      pg_trigger->GetTriggers(txn, table_oid);
  for (int i = 0; i < trigger_lists->GetTriggerListSize(); i++)
    pg_trigger->DropTrigger(txn,
                            database_oid,
                            table_oid,
                            trigger_lists->Get(i)->GetTriggerName());

  // delete record in pg_constraint
  auto pg_constraint =
      catalog_map_[database_object->GetDatabaseOid()]->GetConstraintCatalog();
  pg_constraint->DeleteConstraints(txn, table_oid);

  // delete index and records pg_index
  for (auto it : index_objects)
    DropIndex(txn, database_oid, it.second->GetIndexOid());

  // delete record in pg_attribute
  auto pg_attribute =
      catalog_map_[database_object->GetDatabaseOid()]->GetColumnCatalog();
  pg_attribute->DeleteColumns(txn, table_oid);

  // delete record in pg_layout
  auto pg_layout =
      catalog_map_[database_object->GetDatabaseOid()]->GetLayoutCatalog();
  pg_layout->DeleteLayouts(txn, table_oid);

  // delete record in pg_table
  auto pg_table =
      catalog_map_[database_object->GetDatabaseOid()]->GetTableCatalog();
  pg_table->DeleteTable(txn, table_oid);
  database->GetTableWithOid(table_oid);
  txn->RecordDrop(database_oid, table_oid, INVALID_OID);

  return ResultType::SUCCESS;
}

/*@brief   Drop Index on table
 * @param   index_oid      the oid of the index to be dropped
 * @param   txn            TransactionContext
 * @return  TransactionContext ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropIndex(concurrency::TransactionContext *txn,
                              oid_t database_oid,
                              oid_t index_oid) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop index " +
        std::to_string(index_oid));
  // find index catalog object by looking up pg_index or read from cache using
  // index_oid
  auto pg_index = catalog_map_[database_oid]->GetIndexCatalog();
  auto index_object =
      pg_index->GetIndexCatalogEntry(txn, database_oid, index_oid);
  if (index_object == nullptr) {
    throw CatalogException("Can't find index " + std::to_string(index_oid) +
        " to drop");
  }

  auto storage_manager = storage::StorageManager::GetInstance();
  auto table = storage_manager->GetTableWithOid(database_oid,
                                                index_object->GetTableOid());
  // drop record in pg_index
  pg_index->DeleteIndex(txn, database_oid, index_oid);
  LOG_TRACE("Successfully drop index %d for table %s", index_oid,
            table->GetName().c_str());

  // register index object in rw_object_set
  table->GetIndexWithOid(index_oid);
  txn->RecordDrop(database_oid, index_object->GetTableOid(), index_oid);

  return ResultType::SUCCESS;
}

ResultType Catalog::DropLayout(concurrency::TransactionContext *txn,
                               oid_t database_oid,
                               oid_t table_oid,
                               oid_t layout_oid) {
  // Check if the default_layout of the table is the same.
  // If true reset it to a row store.
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto table = database->GetTableWithOid(table_oid);
  auto default_layout = table->GetDefaultLayout();

  auto pg_layout = catalog_map_[database_oid]->GetLayoutCatalog();
  if (!pg_layout->DeleteLayout(txn, table_oid, layout_oid)) {
    auto layout = table->GetDefaultLayout();
    LOG_DEBUG("Layout delete failed. Default layout id: %u", layout->GetOid());
    return ResultType::FAILURE;
  }

  if (default_layout->GetOid() == layout_oid) {
    table->ResetDefaultLayout();
    auto new_default_layout = table->GetDefaultLayout();
    if (pg_layout->GetLayoutWithOid(txn,
                                    table_oid,
                                    new_default_layout->GetOid()) == nullptr &&
        !pg_layout->InsertLayout(txn,
                                 table_oid,
                                 new_default_layout,
                                 pool_.get())) {
      LOG_DEBUG("Failed to create a new layout for table %d", table_oid);
      return ResultType::FAILURE;
    }

    // update table catalog
    catalog_map_[database_oid]->GetTableCatalog()
        ->UpdateDefaultLayoutOid(txn, table_oid, new_default_layout->GetOid());
  }

  return ResultType::SUCCESS;
}

/**
 * @brief   Drop not null constraint for a column
 * @param   txn           TransactionContext
 * @param   database_oid  the database to which the table belongs to
 * @param   table_oid     the table to which the column belongs to
 * @param   column_id     the column which has the not null constraint
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropNotNullConstraint(concurrency::TransactionContext *txn,
                                          oid_t database_oid,
                                          oid_t table_oid,
                                          oid_t column_id) {
  auto table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();
  auto column = schema->GetColumn(column_id);

  // Check not null
  if (!column.IsNotNull()) {
    throw CatalogException("Column " + column.GetName() + " in table " +
                           table_object->GetTableName() + " isn't NOT NULL.");
  }

  // Update pg_column to set constraint of the column
  auto pg_column = catalog_map_[database_oid]->GetColumnCatalog();
  pg_column->UpdateNotNullConstraint(txn,
                                     table_oid,
                                     column.GetName(),
                                     false);

  // Set not null constraint in the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->DropNotNull(column_id);
  }

  return ResultType::SUCCESS;
}

/**
 * @brief   Drop default constraint for a column
 * @param   txn           TransactionContext
 * @param   database_oid  the database to which the table belongs to
 * @param   table_oid     the table to which the column belongs to
 * @param   column_id     the column which has the default constraint
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropDefaultConstraint(concurrency::TransactionContext *txn,
                                          oid_t database_oid,
                                          oid_t table_oid,
                                          oid_t column_id) {
  auto table_object =
      catalog_map_[database_oid]->GetTableCatalog()
                                ->GetTableCatalogEntry(txn, table_oid);
  auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(database_oid, table_oid)
                    ->GetSchema();
  auto column = schema->GetColumn(column_id);

  // Check default
  if (!column.HasDefault()) {
    throw CatalogException("Column " + column.GetName() + " in table" +
                           table_object->GetTableName() +
                           " doesn't have default value.");
  }

  // Update pg_column to set constraint of the column
  auto pg_column = catalog_map_[database_oid]->GetColumnCatalog();
  pg_column->UpdateDefaultConstraint(txn,
                                     table_oid,
                                     column.GetName(),
                                     false,
                                     nullptr);

  // Set default constraint in the schema
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    schema->DropDefaultValue(column_id);
  }

  return ResultType::SUCCESS;
}

/**
 * @brief   Drop a constraint for a table
 * @param   txn           TransactionContext
 * @param   database_oid  the database to which the table belongs to
 * @param   table_oid     the table which has the constraint
 * @param   constraint_oid    the constraint to be dropped
 * @return  ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropConstraint(concurrency::TransactionContext *txn,
                                   oid_t database_oid,
                                   oid_t table_oid,
                                   oid_t constraint_oid) {
  auto pg_constraint = catalog_map_[database_oid]->GetConstraintCatalog();
  auto constraint_object =
      pg_constraint->GetConstraintCatalogEntry(txn,
                                               table_oid,
                                               constraint_oid);

  // delete constraint object from pg_constraint
  if (!pg_constraint->DeleteConstraint(txn, table_oid, constraint_oid)) {
    throw CatalogException("Failed to delete the constraint: " +
                           std::to_string(constraint_oid));
  }

  // delete index if exists
  if (constraint_object->GetIndexOid() != INVALID_OID) {
    DropIndex(txn, database_oid, constraint_object->GetIndexOid());
  }

  // delete constraint from table
  auto storage_manager = storage::StorageManager::GetInstance();
  auto table = storage_manager->GetTableWithOid(database_oid, table_oid);
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    table->GetSchema()->DropConstraint(constraint_oid);
  }

  // delete foreign key info from sink table
  if (constraint_object->GetConstraintType() == ConstraintType::FOREIGN) {
    auto sink_table =
        storage_manager->GetTableWithOid(database_oid,
                                         constraint_object->GetFKSinkTableOid());
    {
      std::lock_guard<std::mutex> lock(catalog_mutex);
      sink_table->GetSchema()
                ->DeleteForeignKeySource(constraint_object->GetConstraintOid());
    }
  }

  LOG_TRACE(
      "Drop a %s constraint in %s.",
      ConstraintTypeToString(constraint_object->GetConstraintType()).c_str(),
      catalog_map_[database_oid]
          ->GetTableCatalog()
          ->GetTableCatalogEntry(txn, table_oid)
          ->GetTableName()
          .c_str());

  return ResultType::SUCCESS;
}

//===--------------------------------------------------------------------===//
// GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
//===--------------------------------------------------------------------===//

/* Check database from pg_database with database_name using txn,
 * get it from storage layer using database_oid,
 * throw exception and abort txn if not exists/invisible
 * */
storage::Database *Catalog::GetDatabaseWithName(concurrency::TransactionContext *txn,
                                                const std::string &database_name) const {
  PELOTON_ASSERT(txn != nullptr);

  // Check in pg_database using txn
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);

  if (database_object == nullptr) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetDatabaseWithOid(database_object->GetDatabaseOid());
}

/* Check table from pg_table with table_name & schema_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
storage::DataTable *Catalog::GetTableWithName(concurrency::TransactionContext *txn,
                                              const std::string &database_name,
                                              const std::string &schema_name,
                                              const std::string &table_name) {
  PELOTON_ASSERT(txn != nullptr);
  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
            database_name.c_str());

  // Check in pg_table, throw exception and abort txn if not exists
  auto table_object =
      GetTableCatalogEntry(txn, database_name, schema_name, table_name);

  // Get table from storage manager
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTableWithOid(table_object->GetDatabaseOid(),
                                          table_object->GetTableOid());
}

/* Check table from pg_table with table_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
std::shared_ptr<DatabaseCatalogEntry> Catalog::GetDatabaseCatalogEntry(
    concurrency::TransactionContext *txn,
    const std::string &database_name) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
        database_name);
  }

  LOG_TRACE("Looking for database %s", database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);

  if (!database_object || database_object->GetDatabaseOid() == INVALID_OID) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  return database_object;
}

std::shared_ptr<DatabaseCatalogEntry> Catalog::GetDatabaseCatalogEntry(
    concurrency::TransactionContext *txn,
    oid_t database_oid) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get database object " +
        std::to_string(database_oid));
  }

  LOG_TRACE("Looking for database %u", database_oid);

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_oid);

  if (!database_object || database_object->GetDatabaseOid() == INVALID_OID) {
    throw CatalogException("Database " + std::to_string(database_oid) +
        " is not found");
  }

  return database_object;
}

/* Check table from pg_table with table_name  & schema_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
std::shared_ptr<TableCatalogEntry> Catalog::GetTableCatalogEntry(concurrency::TransactionContext *txn,
                                                                 const std::string &database_name,
                                                                 const std::string &schema_name,
                                                                 const std::string &table_name) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
        database_name + "." + table_name);
  }

  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
            database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_name);

  if (!database_object || database_object->GetDatabaseOid() == INVALID_OID) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  // Check in pg_table using txn
  auto table_object =
      database_object->GetTableCatalogEntry(table_name, schema_name);

  if (!table_object || table_object->GetTableOid() == INVALID_OID) {
    // throw table not found exception and explicitly abort txn
    throw CatalogException("Table " + schema_name + "." + table_name +
        " is not found");
  }

  return table_object;
}

std::shared_ptr<TableCatalogEntry> Catalog::GetTableCatalogEntry(concurrency::TransactionContext *txn,
                                                                 oid_t database_oid,
                                                                 oid_t table_oid) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
        std::to_string(database_oid) + "." +
        std::to_string(table_oid));
  }

  LOG_TRACE("Looking for table %u in database %u", table_oid, database_oid);

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_oid);

  if (!database_object || database_object->GetDatabaseOid() == INVALID_OID) {
    throw CatalogException("Database " + std::to_string(database_oid) +
        " is not found");
  }

  // Check in pg_table using txn
  auto table_object = database_object->GetTableCatalogEntry(table_oid);

  if (!table_object || table_object->GetTableOid() == INVALID_OID) {
    // throw table not found exception and explicitly abort txn
    throw CatalogException("Table " + std::to_string(table_oid) +
        " is not found");
  }

  return table_object;
}

std::shared_ptr<SystemCatalogs> Catalog::GetSystemCatalogs(
    const oid_t database_oid) {
  if (catalog_map_.find(database_oid) == catalog_map_.end()) {
    throw CatalogException("Failed to find SystemCatalog for database_oid = " +
        std::to_string(database_oid));
  }
  return catalog_map_[database_oid];
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This should be deprecated! this can screw up the database oid system
void Catalog::AddDatabase(storage::Database *database) {
  std::lock_guard<std::mutex> lock(catalog_mutex);
  auto storage_manager = storage::StorageManager::GetInstance();
  storage_manager->AddDatabaseToStorageManager(database);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  BootstrapSystemCatalogs(txn, database);
  DatabaseCatalog::GetInstance(nullptr, nullptr, nullptr)->InsertDatabase(txn,
                                                                          database->GetOid(),
                                                                          database->GetDBName(),
                                                                          pool_.get());  // I guess this can pass tests
  txn_manager.CommitTransaction(txn);
}

//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

Catalog::~Catalog() {
  storage::StorageManager::GetInstance()->DestroyDatabases();
}

//===--------------------------------------------------------------------===//
// FUNCTION
//===--------------------------------------------------------------------===//

/* @brief
 * Add a new built-in function. This proceeds in two steps:
 *   1. Add the function information into pg_catalog.pg_proc
 *   2. Register the function pointer in function::BuiltinFunction
 * @param   name & argument_types   function name and arg types used in SQL
 * @param   return_type   the return type
 * @param   prolang       the oid of which language the function is
 * @param   func_name     the function name in C++ source (should be unique)
 * @param   func_ptr      the pointer to the function
 */
void Catalog::AddBuiltinFunction(concurrency::TransactionContext *txn,
                                 const std::string &name,
                                 function::BuiltInFuncType func,
                                 const std::string &func_name,
                                 type::TypeId return_type,
                                 const std::vector<type::TypeId> &argument_types,
                                 oid_t prolang) {
  if (!ProcCatalog::GetInstance().InsertProc(txn,
                                             name,
                                             return_type,
                                             argument_types,
                                             prolang,
                                             func_name,
                                             pool_.get())) {
    throw CatalogException("Failed to add function " + func_name);
  }
  function::BuiltInFunctions::AddFunction(func_name, func);
}

/* @brief
 * Add a new plpgsql function. This proceeds in two steps:
 *   1. Add the function information into pg_catalog.pg_proc
 *   2. Register the function code_context in function::PlgsqlFunction
 * @param   name & argument_types   function name and arg types used in SQL
 * @param   return_type   the return type
 * @param   prolang       the oid of which language the function is
 * @param   func_src      the plpgsql UDF function body
 * @details func_src can be used to reconstruct the llvm code_context in case
 *          of failures
 * @param   code_context  the code_context that holds the generated LLVM
 *                        query code
 */
void Catalog::AddProcedure(concurrency::TransactionContext *txn,
                           const std::string &name,
                           type::TypeId return_type,
                           const std::vector<type::TypeId> &argument_types,
                           oid_t prolang,
                           std::shared_ptr<peloton::codegen::CodeContext> code_context,
                           const std::string &func_src) {
  // Check if UDF already exists
  auto proc_catalog_obj =
      ProcCatalog::GetInstance().GetProcByName(txn, name, argument_types);

  if (proc_catalog_obj == nullptr) {
    if (!ProcCatalog::GetInstance().InsertProc(txn,
                                               name,
                                               return_type,
                                               argument_types,
                                               prolang,
                                               func_src,
                                               pool_.get())) {
      throw CatalogException("Failed to add function " + name);
    }
    proc_catalog_obj =
        ProcCatalog::GetInstance().GetProcByName(txn, name, argument_types);
    // Insert UDF into Catalog
    function::PlpgsqlFunctions::AddFunction(proc_catalog_obj->GetOid(),
                                            code_context);
  }
}

const FunctionData Catalog::GetFunction(
    const std::string &name, const std::vector<type::TypeId> &argument_types) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Lookup the function in pg_proc
  auto &proc_catalog = ProcCatalog::GetInstance();
  auto proc_catalog_obj = proc_catalog.GetProcByName(txn, name, argument_types);
  if (proc_catalog_obj == nullptr) {
    txn_manager.AbortTransaction(txn);
    throw CatalogException("Failed to find function " + name);
  }

  // If the language isn't 'internal' or 'plpgsql', crap out ... for now ...
  auto lang_catalog_obj = proc_catalog_obj->GetLanguage();
  if (lang_catalog_obj == nullptr ||
      (lang_catalog_obj->GetName() != "internal" &&
          lang_catalog_obj->GetName() != "plpgsql")) {
    txn_manager.AbortTransaction(txn);
    throw CatalogException(
        "Peloton currently only supports internal functions and plpgsql UDFs. \
        Function " +
            name + " has language '" + lang_catalog_obj->GetName() + "'");
  }

  FunctionData result;
  result.argument_types_ = argument_types;
  result.func_name_ = proc_catalog_obj->GetSrc();
  result.return_type_ = proc_catalog_obj->GetRetType();
  if (lang_catalog_obj->GetName() == "internal") {
    // If the function is "internal", perform the lookup in our built-in
    // functions map (i.e., function::BuiltInFunctions) to get the function
    result.func_ = function::BuiltInFunctions::GetFuncByName(result.func_name_);
    result.is_udf_ = false;
    if (result.func_.impl == nullptr) {
      txn_manager.AbortTransaction(txn);
      throw CatalogException(
          "Function " + name +
              " is internal, but doesn't have a function address");
    }
  } else if (lang_catalog_obj->GetName() == "plpgsql") {
    // If the function is a "plpgsql" udf, perform the lookup in the plpgsql
    // functions map (i.e., function::PlpgsqlFunctions) to get the function
    // code_context
    result.func_context_ = function::PlpgsqlFunctions::GetFuncContextByOid(
        proc_catalog_obj->GetOid());
    result.is_udf_ = true;

    if (result.func_context_->GetUDF() == nullptr) {
      txn_manager.AbortTransaction(txn);
      throw CatalogException(
          "Function " + name +
              " is plpgsql, but doesn't have a function address");
    }
  }

  txn_manager.CommitTransaction(txn);
  return result;
}

void Catalog::InitializeLanguages() {
  static bool initialized = false;
  if (!initialized) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // add "internal" language
    if (!LanguageCatalog::GetInstance().InsertLanguage(txn,
                                                       "internal",
                                                       pool_.get())) {
      txn_manager.AbortTransaction(txn);
      throw CatalogException("Failed to add language 'internal'");
    }
    // Add "plpgsql" language
    if (!LanguageCatalog::GetInstance().InsertLanguage(txn,
                                                       "plpgsql",
                                                       pool_.get())) {
      txn_manager.AbortTransaction(txn);
      throw CatalogException("Failed to add language 'plpgsql'");
    }
    txn_manager.CommitTransaction(txn);
    initialized = true;
  }
}

void Catalog::InitializeFunctions() {
  static bool initialized = false;
  if (!initialized) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    auto lang_object =
        LanguageCatalog::GetInstance().GetLanguageByName(txn, "internal");
    if (lang_object == nullptr) {
      throw CatalogException("Language 'internal' does not exist");
    }
    oid_t internal_lang = lang_object->GetOid();

    try {
      /**
       * string functions
       */
      AddBuiltinFunction(txn,
                         "ascii",
                         function::BuiltInFuncType{OperatorId::Ascii,
                                                   function::OldEngineStringFunctions::Ascii},
                         "Ascii",
                         type::TypeId::INTEGER,
                         {type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "chr",
                         function::BuiltInFuncType{OperatorId::Chr,
                                                   function::OldEngineStringFunctions::Chr},
                         "Chr",
                         type::TypeId::VARCHAR,
                         {type::TypeId::INTEGER},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "concat",
                         function::BuiltInFuncType{OperatorId::Concat,
                                                   function::OldEngineStringFunctions::Concat},
                         "Concat",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "substr",
                         function::BuiltInFuncType{OperatorId::Substr,
                                                   function::OldEngineStringFunctions::Substr},
                         "Substr",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::INTEGER,
                          type::TypeId::INTEGER},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "char_length",
                         function::BuiltInFuncType{
                             OperatorId::CharLength,
                             function::OldEngineStringFunctions::CharLength},
                         "CharLength",
                         type::TypeId::INTEGER,
                         {type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "octet_length",
                         function::BuiltInFuncType{
                             OperatorId::OctetLength,
                             function::OldEngineStringFunctions::OctetLength},
                         "OctetLength",
                         type::TypeId::INTEGER,
                         {type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "length",
                         function::BuiltInFuncType{OperatorId::Length,
                                                   function::OldEngineStringFunctions::Length},
                         "Length",
                         type::TypeId::INTEGER,
                         {type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "repeat",
                         function::BuiltInFuncType{OperatorId::Repeat,
                                                   function::OldEngineStringFunctions::Repeat},
                         "Repeat",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "replace",
                         function::BuiltInFuncType{
                             OperatorId::Replace,
                             function::OldEngineStringFunctions::Replace},
                         "Replace",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR,
                          type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "ltrim",
                         function::BuiltInFuncType{OperatorId::LTrim,
                                                   function::OldEngineStringFunctions::LTrim},
                         "LTrim",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "rtrim",
                         function::BuiltInFuncType{OperatorId::RTrim,
                                                   function::OldEngineStringFunctions::RTrim},
                         "RTrim",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "btrim",
                         function::BuiltInFuncType{OperatorId::BTrim,
                                                   function::OldEngineStringFunctions::BTrim},
                         "btrim",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                         internal_lang);
      // Trim
      AddBuiltinFunction(txn,
                         "btrim",
                         function::BuiltInFuncType{OperatorId::Trim,
                                                   function::OldEngineStringFunctions::Trim},
                         "trim",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "like",
                         function::BuiltInFuncType{OperatorId::Like,
                                                   function::OldEngineStringFunctions::Like},
                         "like",
                         type::TypeId::VARCHAR,
                         {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                         internal_lang);

      /**
       * decimal functions
       */
      AddBuiltinFunction(txn,
                         "abs",
                         function::BuiltInFuncType{
                             OperatorId::Abs, function::NumericFunctions::_Abs},
                         "Abs",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "sqrt",
                         function::BuiltInFuncType{OperatorId::Sqrt,
                                                   function::NumericFunctions::Sqrt},
                         "Sqrt",
                         type::TypeId::DECIMAL,
                         {type::TypeId::TINYINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "sqrt",
                         function::BuiltInFuncType{OperatorId::Sqrt,
                                                   function::NumericFunctions::Sqrt},
                         "Sqrt",
                         type::TypeId::DECIMAL,
                         {type::TypeId::SMALLINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "sqrt",
                         function::BuiltInFuncType{OperatorId::Sqrt,
                                                   function::NumericFunctions::Sqrt},
                         "Sqrt",
                         type::TypeId::DECIMAL,
                         {type::TypeId::INTEGER},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "sqrt",
                         function::BuiltInFuncType{OperatorId::Sqrt,
                                                   function::NumericFunctions::Sqrt},
                         "Sqrt",
                         type::TypeId::DECIMAL,
                         {type::TypeId::BIGINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "sqrt",
                         function::BuiltInFuncType{OperatorId::Sqrt,
                                                   function::NumericFunctions::Sqrt},
                         "Sqrt",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "floor",
                         function::BuiltInFuncType{OperatorId::Floor,
                                                   function::NumericFunctions::_Floor},
                         "Floor",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);

      /**
       * integer functions
       */
      AddBuiltinFunction(txn,
                         "abs",
                         function::BuiltInFuncType{
                             OperatorId::Abs, function::NumericFunctions::_Abs},
                         "Abs",
                         type::TypeId::TINYINT,
                         {type::TypeId::TINYINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "abs",
                         function::BuiltInFuncType{
                             OperatorId::Abs, function::NumericFunctions::_Abs},
                         "Abs",
                         type::TypeId::SMALLINT,
                         {type::TypeId::SMALLINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "abs",
                         function::BuiltInFuncType{
                             OperatorId::Abs, function::NumericFunctions::_Abs},
                         "Abs",
                         type::TypeId::INTEGER,
                         {type::TypeId::INTEGER},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "abs",
                         function::BuiltInFuncType{
                             OperatorId::Abs, function::NumericFunctions::_Abs},
                         "Abs",
                         type::TypeId::BIGINT,
                         {type::TypeId::BIGINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "floor",
                         function::BuiltInFuncType{OperatorId::Floor,
                                                   function::NumericFunctions::_Floor},
                         "Floor",
                         type::TypeId::DECIMAL,
                         {type::TypeId::INTEGER},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "floor",
                         function::BuiltInFuncType{OperatorId::Floor,
                                                   function::NumericFunctions::_Floor},
                         "Floor",
                         type::TypeId::DECIMAL,
                         {type::TypeId::BIGINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "floor",
                         function::BuiltInFuncType{OperatorId::Floor,
                                                   function::NumericFunctions::_Floor},
                         "Floor",
                         type::TypeId::DECIMAL,
                         {type::TypeId::TINYINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "floor",
                         function::BuiltInFuncType{OperatorId::Floor,
                                                   function::NumericFunctions::_Floor},
                         "Floor",
                         type::TypeId::DECIMAL,
                         {type::TypeId::SMALLINT},
                         internal_lang);
      AddBuiltinFunction(txn,
                         "round",
                         function::BuiltInFuncType{OperatorId::Round,
                                                   function::NumericFunctions::_Round},
                         "Round",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceil",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceil",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::TINYINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceil",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::SMALLINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceil",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::INTEGER},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceil",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::BIGINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceiling",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::DECIMAL},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceiling",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::TINYINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceiling",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::SMALLINT},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceiling",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::INTEGER},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "ceiling",
                         function::BuiltInFuncType{OperatorId::Ceil,
                                                   function::NumericFunctions::_Ceil},
                         "Ceil",
                         type::TypeId::DECIMAL,
                         {type::TypeId::BIGINT},
                         internal_lang);

      /**
       * date functions
       */
      AddBuiltinFunction(txn,
                         "date_part",
                         function::BuiltInFuncType{OperatorId::DatePart,
                                                   function::TimestampFunctions::_DatePart},
                         "DatePart",
                         type::TypeId::DECIMAL,
                         {type::TypeId::VARCHAR, type::TypeId::TIMESTAMP},
                         internal_lang);

      AddBuiltinFunction(txn,
                         "date_trunc",
                         function::BuiltInFuncType{OperatorId::DateTrunc,
                                                   function::TimestampFunctions::_DateTrunc},
                         "DateTrunc",
                         type::TypeId::TIMESTAMP,
                         {type::TypeId::VARCHAR, type::TypeId::TIMESTAMP},
                         internal_lang);

      // add now()
      AddBuiltinFunction(txn,
                         "now",
                         function::BuiltInFuncType{
                             OperatorId::Now, function::DateFunctions::_Now},
                         "Now",
                         type::TypeId::TIMESTAMP,
                         {},
                         internal_lang);

    } catch (CatalogException &e) {
      txn_manager.AbortTransaction(txn);
      throw e;
    }
    txn_manager.CommitTransaction(txn);
    initialized = true;
  }
}

}  // namespace catalog
}  // namespace peloton
