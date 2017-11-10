//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/catalog/catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===---------------------------------------------------------------------===//

#include "catalog/catalog.h"

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/settings_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/trigger_catalog.h"
#include "catalog/proc_catalog.h"
#include "catalog/language_catalog.h"
#include "function/date_functions.h"
#include "function/decimal_functions.h"
#include "function/string_functions.h"
#include "index/index_factory.h"
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
 * 1) create pg_catalog database, create catalog tables, add them into
 * pg_catalog database, insert columns into pg_attribute
 * 2) create necessary indexes, insert into pg_index
 * 3) insert pg_catalog into pg_database, catalog tables into pg_table
 */
Catalog::Catalog() : pool_(new type::EphemeralPool()) {
  // Begin transaction for catalog initialization
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto storage_manager = storage::StorageManager::GetInstance();
  // Create pg_catalog database
  auto pg_catalog = new storage::Database(CATALOG_DATABASE_OID);
  pg_catalog->setDBName(CATALOG_DATABASE_NAME);
  storage_manager->AddDatabaseToStorageManager(pg_catalog);

  // Create catalog tables
  auto pg_database = DatabaseCatalog::GetInstance(pg_catalog, pool_.get(), txn);
  auto pg_table = TableCatalog::GetInstance(pg_catalog, pool_.get(), txn);
  IndexCatalog::GetInstance(pg_catalog, pool_.get(), txn);
  //  ColumnCatalog::GetInstance(); // Called implicitly

  // Create indexes on catalog tables, insert them into pg_index
  // note that CreateIndex() from catalog.cpp will create index on storage level
  // table and at the same time insert a new index record into pg_index
  // TODO: This should be hash index rather than tree index?? (but postgres use
  // btree!!)

  CreatePrimaryIndex(CATALOG_DATABASE_OID, DATABASE_CATALOG_OID, txn);
  CreatePrimaryIndex(CATALOG_DATABASE_OID, TABLE_CATALOG_OID, txn);

  CreateIndex(CATALOG_DATABASE_OID, DATABASE_CATALOG_OID,
              {DatabaseCatalog::ColumnId::DATABASE_NAME},
              DATABASE_CATALOG_NAME "_skey0", IndexType::BWTREE,
              IndexConstraintType::UNIQUE, true, txn, true);

  CreateIndex(CATALOG_DATABASE_OID, TABLE_CATALOG_OID,
              {TableCatalog::ColumnId::TABLE_NAME,
               TableCatalog::ColumnId::DATABASE_OID},
              TABLE_CATALOG_NAME "_skey0", IndexType::BWTREE,
              IndexConstraintType::UNIQUE, true, txn, true);
  CreateIndex(CATALOG_DATABASE_OID, TABLE_CATALOG_OID,
              {TableCatalog::ColumnId::DATABASE_OID},
              TABLE_CATALOG_NAME "_skey1", IndexType::BWTREE,
              IndexConstraintType::DEFAULT, false, txn, true);

  // actual index already added in column_catalog, index_catalog constructor
  // the reason we treat those two catalog tables differently is that indexes
  // needs to be built before insert tuples into table
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey", COLUMN_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, true,
      {ColumnCatalog::ColumnId::TABLE_OID,
       ColumnCatalog::ColumnId::COLUMN_NAME},
      pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_SKEY0_OID, COLUMN_CATALOG_NAME "_skey0",
      COLUMN_CATALOG_OID, IndexType::BWTREE, IndexConstraintType::UNIQUE, true,
      {ColumnCatalog::ColumnId::TABLE_OID, ColumnCatalog::ColumnId::COLUMN_ID},
      pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_SKEY1_OID, COLUMN_CATALOG_NAME "_skey1",
      COLUMN_CATALOG_OID, IndexType::BWTREE, IndexConstraintType::DEFAULT,
      false, {ColumnCatalog::ColumnId::TABLE_OID}, pool_.get(), txn);

  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_PKEY_OID, INDEX_CATALOG_NAME "_pkey", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, true,
      {IndexCatalog::ColumnId::INDEX_OID}, pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_SKEY0_OID, INDEX_CATALOG_NAME "_skey0", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::UNIQUE, true,
      {IndexCatalog::ColumnId::INDEX_NAME}, pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_SKEY1_OID, INDEX_CATALOG_NAME "_skey1", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::DEFAULT, false,
      {IndexCatalog::ColumnId::TABLE_OID}, pool_.get(), txn);

  // Insert pg_catalog database into pg_database
  pg_database->InsertDatabase(CATALOG_DATABASE_OID, CATALOG_DATABASE_NAME,
                              pool_.get(), txn);

  // Insert catalog tables into pg_table
  pg_table->InsertTable(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                        CATALOG_DATABASE_OID, pool_.get(), txn);
  pg_table->InsertTable(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                        CATALOG_DATABASE_OID, pool_.get(), txn);
  pg_table->InsertTable(INDEX_CATALOG_OID, INDEX_CATALOG_NAME,
                        CATALOG_DATABASE_OID, pool_.get(), txn);
  pg_table->InsertTable(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                        CATALOG_DATABASE_OID, pool_.get(), txn);

  // Commit transaction
  txn_manager.CommitTransaction(txn);
}

void Catalog::Bootstrap() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  DatabaseMetricsCatalog::GetInstance(txn);
  TableMetricsCatalog::GetInstance(txn);
  IndexMetricsCatalog::GetInstance(txn);
  QueryMetricsCatalog::GetInstance(txn);
  SettingsCatalog::GetInstance(txn);
  TriggerCatalog::GetInstance(txn);
  LanguageCatalog::GetInstance(txn);
  ProcCatalog::GetInstance(txn);

  txn_manager.CommitTransaction(txn);

  InitializeLanguages();
  InitializeFunctions();
}

//===----------------------------------------------------------------------===//
// CREATE FUNCTIONS
//===----------------------------------------------------------------------===//

ResultType Catalog::CreateDatabase(const std::string &database_name,
                                   concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create database " +
                           database_name);

  auto pg_database = DatabaseCatalog::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();
  // Check if a database with the same name exists
  auto database_object = pg_database->GetDatabaseObject(database_name, txn);
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
  pg_database->InsertDatabase(database_oid, database_name, pool_.get(), txn);

  LOG_TRACE("Database %s created. Returning RESULT_SUCCESS.",
            database_name.c_str());
  return ResultType::SUCCESS;
}

/*@brief   create table
 * @param   database_name    the database which the table belongs to
 * @param   table_name       name of the table to add index on
 * @param   schema           schema, a.k.a metadata of the table
 * @param   txn              Transaction
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateTable(const std::string &database_name,
                                const std::string &table_name,
                                std::unique_ptr<catalog::Schema> schema,
                                concurrency::Transaction *txn,
                                bool is_catalog) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create table " +
                           table_name);

  LOG_TRACE("Creating table %s in database %s", table_name.c_str(),
            database_name.c_str());
  // get database oid from pg_database
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);
  if (database_object == nullptr)
    throw CatalogException("Can't find Database " + database_name +
                           " to create table");

  // get table oid from pg_table
  auto table_object = database_object->GetTableObject(table_name);
  if (table_object != nullptr)
    throw CatalogException("Table " + table_name + " already exists");

  auto storage_manager = storage::StorageManager::GetInstance();
  auto database =
      storage_manager->GetDatabaseWithOid(database_object->database_oid);

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
  auto pg_table = TableCatalog::GetInstance();
  oid_t table_oid = pg_table->GetNextOid();
  bool own_schema = true;
  bool adapt_table = false;
  auto table = storage::TableFactory::GetDataTable(
      database_object->database_oid, table_oid, schema.release(), table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table, is_catalog);
  database->AddTable(table, is_catalog);
  // put data table object into rw_object_set
  txn->RecordCreate(database_object->database_oid, table_oid, INVALID_OID);

  // Update pg_table with table info
  pg_table->InsertTable(table_oid, table_name, database_object->database_oid,
                        pool_.get(), txn);
  oid_t column_id = 0;
  for (const auto &column : table->GetSchema()->GetColumns()) {
    ColumnCatalog::GetInstance()->InsertColumn(
        table_oid, column.GetName(), column_id, column.GetOffset(),
        column.GetType(), column.IsInlined(), column.GetConstraints(),
        pool_.get(), txn);
    column_id++;

    // Create index on unique single column
    if (column.IsUnique()) {
      std::string col_name = column.GetName();
      std::string index_name = table->GetName() + "_" + col_name + "_UNIQ";
      CreateIndex(database_name, table_name, {column_id}, index_name, true,
                  IndexType::BWTREE, txn);
      LOG_DEBUG("Added a UNIQUE index on %s in %s.", col_name.c_str(),
                table_name.c_str());
    }
  }
  CreatePrimaryIndex(database_object->database_oid, table_oid, txn);
  return ResultType::SUCCESS;
}

/*@brief   create primary index on table
 * Note that this is a catalog helper function only called within catalog.cpp
 * If you want to create index on table outside, call CreateIndex() instead
 * @param   database_oid     the database which the indexed table belongs to
 * @param   table_oid        oid of the table to add index on
 * @param   txn              Transaction
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreatePrimaryIndex(oid_t database_oid, oid_t table_oid,
                                       concurrency::Transaction *txn) {
  LOG_TRACE("Trying to create primary index for table %d", table_oid);

  auto storage_manager = storage::StorageManager::GetInstance();

  auto database = storage_manager->GetDatabaseWithOid(database_oid);

  auto table = database->GetTableWithOid(table_oid);

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

  if (key_attrs.empty()) return ResultType::FAILURE;

  key_schema = catalog::Schema::CopySchema(schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  std::string index_name = table->GetName() + "_pkey";

  bool unique_keys = true;
  oid_t index_oid = IndexCatalog::GetInstance()->GetNextOid();

  index_metadata = new index::IndexMetadata(
      index_name, index_oid, table_oid, database_oid, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, schema, key_schema, key_attrs,
      unique_keys);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  table->AddIndex(pkey_index);

  // put index object into rw_object_set
  txn->RecordCreate(database_oid, table_oid, index_oid);
  // insert index record into index_catalog(pg_index) table
  IndexCatalog::GetInstance()->InsertIndex(
      index_oid, index_name, table_oid, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, unique_keys, key_attrs, pool_.get(),
      txn);

  LOG_TRACE("Successfully created primary key index '%s' for table '%s'",
            index_name.c_str(), table->GetName().c_str());

  return ResultType::SUCCESS;
}

/*@brief   create index on table
 * @param   database_name    the database which the indexed table belongs to
 * @param   table_name       name of the table to add index on
 * @param   index_attr       collection of the indexed attribute(column) name
 * @param   index_name       name of the table to add index on
 * @param   unique_keys      index supports duplicate key or not
 * @param   index_type       the type of index(default value is BWTREE)
 * @param   txn              Transaction
 * @param   is_catalog       index is built on catalog table or not(useful in
 * catalog table Initialization)
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::CreateIndex(const std::string &database_name,
                                const std::string &table_name,
                                const std::vector<oid_t> &key_attrs,
                                const std::string &index_name, bool unique_keys,
                                IndexType index_type,
                                concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create database " +
                           index_name);

  LOG_TRACE("Trying to create index %s for table %s", index_name.c_str(),
            table_name.c_str());

  // check if database exists
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);
  if (database_object == nullptr)
    throw CatalogException("Can't find Database " + database_name +
                           " to create index");

  // check if table exists
  auto table_object = database_object->GetTableObject(table_name);
  if (table_object == nullptr)
    throw CatalogException("Can't find table " + table_name +
                           " to create index");

  IndexConstraintType index_constraint =
      unique_keys ? IndexConstraintType::UNIQUE : IndexConstraintType::DEFAULT;

  ResultType success = CreateIndex(
      database_object->database_oid, table_object->table_oid, key_attrs,
      index_name, index_type, index_constraint, unique_keys, txn);

  return success;
}

ResultType Catalog::CreateIndex(oid_t database_oid, oid_t table_oid,
                                const std::vector<oid_t> &key_attrs,
                                const std::string &index_name,
                                IndexType index_type,
                                IndexConstraintType index_constraint,
                                bool unique_keys, concurrency::Transaction *txn,
                                bool is_catalog) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to create index " +
                           index_name);

  LOG_TRACE("Trying to create index for table %d", table_oid);

  if (is_catalog == false) {
    // check if table already has index with same name
    // only check when is_catalog flag == false
    auto database_object =
        DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
    auto table_object = database_object->GetTableObject(table_oid);
    auto index_object = table_object->GetIndexObject(index_name);

    if (index_object != nullptr)
      throw CatalogException("Index " + index_name + " already exists");
  }
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto table = database->GetTableWithOid(table_oid);
  auto schema = table->GetSchema();

  // Passed all checks, now get all index metadata
  LOG_TRACE("Trying to create index %s on table %d", index_name.c_str(),
            table_oid);
  auto pg_index = IndexCatalog::GetInstance();
  oid_t index_oid = pg_index->GetNextOid();
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
  IndexCatalog::GetInstance()->InsertIndex(
      index_oid, index_name, table_oid, index_type, index_constraint,
      unique_keys, key_attrs, pool_.get(), txn);

  LOG_TRACE("Successfully add index for table %s contains %d indexes",
            table->GetName().c_str(), (int)table->GetValidIndexCount());
  return ResultType::SUCCESS;
}

//===----------------------------------------------------------------------===//
// DROP FUNCTIONS
//===----------------------------------------------------------------------===//

/*
 * only for test purposes
 */
ResultType Catalog::DropDatabaseWithName(const std::string &database_name,
                                         concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop database " +
                           database_name);

  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);
  if (database_object == nullptr)
    throw CatalogException("Drop Database: " + database_name +
                           " does not exist");

  return DropDatabaseWithOid(database_object->database_oid, txn);
}

ResultType Catalog::DropDatabaseWithOid(oid_t database_oid,
                                        concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop database " +
                           std::to_string(database_oid));
  auto storage_manager = storage::StorageManager::GetInstance();
  // Drop actual tables in the database
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
  auto table_objects = database_object->GetTableObjects();
  for (auto it : table_objects) {
    DropTable(database_oid, it.second->table_oid, txn);
  }

  // Drop database record in catalog
  if (!DatabaseCatalog::GetInstance()->DeleteDatabase(database_oid, txn))
    throw CatalogException("Database record: " + std::to_string(database_oid) +
                           " does not exist in pg_database");

  // put database object into rw_object_set
  storage_manager->GetDatabaseWithOid(database_oid);
  txn->RecordDrop(database_oid, INVALID_OID, INVALID_OID);

  return ResultType::SUCCESS;
}

/*@brief   Drop table
 * 1. drop all the indexes on actual table, and drop index records in pg_index
 * 2. drop all the columns records in pg_attribute
 * 3. drop table record in pg_table
 * 4. delete actual table(storage level), cleanup schema, foreign keys,
 * tile_groups
 * @param   database_name    the database which the dropped table belongs to
 * @param   table_name       the dropped table name
 * @param   txn              Transaction
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropTable(const std::string &database_name,
                              const std::string &table_name,
                              concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop table " +
                           table_name);

  // Checking if statement is valid
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);
  if (database_object == nullptr)
    throw CatalogException("Drop Table: database " + database_name +
                           " does not exist");

  // check if table exists
  auto table_object = database_object->GetTableObject(table_name);
  if (table_object == nullptr)
    throw CatalogException("Drop Table: table " + table_name +
                           " does not exist");

  ResultType result =
      DropTable(database_object->database_oid, table_object->table_oid, txn);
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
 * @param   txn             Transaction
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropTable(oid_t database_oid, oid_t table_oid,
                              concurrency::Transaction *txn) {
  LOG_TRACE("Dropping table %d from database %d", database_oid, table_oid);
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(database_oid);
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
  auto table_object = database_object->GetTableObject(table_oid);
  auto index_objects = table_object->GetIndexObjects();
  LOG_TRACE("dropping #%d indexes", (int)index_objects.size());

  for (auto it : index_objects) DropIndex(it.second->index_oid, txn);
  ColumnCatalog::GetInstance()->DeleteColumns(table_oid, txn);
  TableCatalog::GetInstance()->DeleteTable(table_oid, txn);

  database->GetTableWithOid(table_oid);
  txn->RecordDrop(database_oid, table_oid, INVALID_OID);

  return ResultType::SUCCESS;
}

/*@brief   Drop Index on table
 * @param   index_oid      the oid of the index to be dropped
 * @param   txn            Transaction
 * @return  Transaction ResultType(SUCCESS or FAILURE)
 */
ResultType Catalog::DropIndex(oid_t index_oid, concurrency::Transaction *txn) {
  if (txn == nullptr)
    throw CatalogException("Do not have transaction to drop index " +
                           std::to_string(index_oid));
  // find index catalog object by looking up pg_index or read from cache using
  // index_oid
  auto index_object =
      IndexCatalog::GetInstance()->GetIndexObject(index_oid, txn);
  if (index_object == nullptr) {
    throw CatalogException("Can't find index " + std::to_string(index_oid) +
                           " to drop");
  }
  // the tricky thing about drop index is that you only know index oid or
  // index
  // table_oid and you must obtain database_object-->table_object in reverse
  // way
  // invalidate index cache object in table_catalog
  auto table_object =
      TableCatalog::GetInstance()->GetTableObject(index_object->table_oid, txn);
  auto storage_manager = storage::StorageManager::GetInstance();
  auto table = storage_manager->GetTableWithOid(table_object->database_oid,
                                                index_object->table_oid);
  // drop record in pg_index
  IndexCatalog::GetInstance()->DeleteIndex(index_oid, txn);
  LOG_TRACE("Successfully drop index %d for table %s", index_oid,
            table->GetName().c_str());

  // register index object in rw_object_set
  table->GetIndexWithOid(index_oid);
  txn->RecordDrop(table_object->database_oid, table_object->table_oid,
                  index_oid);

  return ResultType::SUCCESS;
}

//===--------------------------------------------------------------------===//
// GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
//===--------------------------------------------------------------------===//

/* Check database from pg_database with database_name using txn,
 * get it from storage layer using database_oid,
 * throw exception and abort txn if not exists/invisible
 * */
storage::Database *Catalog::GetDatabaseWithName(
    const std::string &database_name, concurrency::Transaction *txn) const {
  PL_ASSERT(txn != nullptr);

  // Check in pg_database using txn
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);

  if (database_object == nullptr) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetDatabaseWithOid(database_object->database_oid);
}

/* Check table from pg_table with table_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
storage::DataTable *Catalog::GetTableWithName(const std::string &database_name,
                                              const std::string &table_name,
                                              concurrency::Transaction *txn) {
  PL_ASSERT(txn != nullptr);

  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
            database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto table_object = GetTableObject(database_name, table_name, txn);

  // Get table from storage manager
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTableWithOid(table_object->database_oid,
                                          table_object->table_oid);
}

/* Check table from pg_table with table_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
std::shared_ptr<DatabaseCatalogObject> Catalog::GetDatabaseObject(
    const std::string &database_name, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
                           database_name);
  }

  LOG_TRACE("Looking for database %s", database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);

  if (!database_object || database_object->database_oid == INVALID_OID) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  return database_object;
}

std::shared_ptr<DatabaseCatalogObject> Catalog::GetDatabaseObject(
    oid_t database_oid, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get database object " +
                           std::to_string(database_oid));
  }

  LOG_TRACE("Looking for database %u", database_oid);

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);

  if (!database_object || database_object->database_oid == INVALID_OID) {
    throw CatalogException("Database " + std::to_string(database_oid) +
                           " is not found");
  }

  return database_object;
}

/* Check table from pg_table with table_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
std::shared_ptr<TableCatalogObject> Catalog::GetTableObject(
    const std::string &database_name, const std::string &table_name,
    concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
                           database_name + "." + table_name);
  }

  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
            database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_name, txn);

  if (!database_object || database_object->database_oid == INVALID_OID) {
    throw CatalogException("Database " + database_name + " is not found");
  }

  // Check in pg_table using txn
  auto table_object = database_object->GetTableObject(table_name);

  if (!table_object || table_object->table_oid == INVALID_OID) {
    // throw table not found exception and explicitly abort txn
    throw CatalogException("Table " + table_name + " is not found");
  }

  // if (single_statement_txn) {
  //   txn_manager.CommitTransaction(txn);
  // }
  return table_object;
}

std::shared_ptr<TableCatalogObject> Catalog::GetTableObject(
    oid_t database_oid, oid_t table_oid, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    throw CatalogException("Do not have transaction to get table object " +
                           std::to_string(database_oid) + "." +
                           std::to_string(table_oid));
  }

  LOG_TRACE("Looking for table %u in database %u", table_oid, database_oid);

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);

  if (!database_object || database_object->database_oid == INVALID_OID) {
    throw CatalogException("Database " + std::to_string(database_oid) +
                           " is not found");
  }

  // Check in pg_table using txn
  auto table_object = database_object->GetTableObject(table_oid);

  if (!table_object || table_object->table_oid == INVALID_OID) {
    // throw table not found exception and explicitly abort txn
    throw CatalogException("Table " + std::to_string(table_oid) +
                           " is not found");
  }

  return table_object;
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
  DatabaseCatalog::GetInstance()->InsertDatabase(
      database->GetOid(), database->GetDBName(), pool_.get(),
      txn);  // I guess this can pass tests
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
void Catalog::AddBuiltinFunction(
    const std::string &name, const std::vector<type::TypeId> &argument_types,
    const type::TypeId return_type, oid_t prolang, const std::string &func_name,
    function::BuiltInFuncType func, concurrency::Transaction *txn) {
  if (!ProcCatalog::GetInstance().InsertProc(name, return_type, argument_types,
                                             prolang, func_name, pool_.get(),
                                             txn)) {
    throw CatalogException("Failed to add function " + func_name);
  }
  function::BuiltInFunctions::AddFunction(func_name, func);
}

const FunctionData Catalog::GetFunction(
    const std::string &name, const std::vector<type::TypeId> &argument_types) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Lookup the function in pg_proc
  auto &proc_catalog = ProcCatalog::GetInstance();
  auto proc_catalog_obj = proc_catalog.GetProcByName(name, argument_types, txn);
  if (proc_catalog_obj == nullptr) {
    txn_manager.AbortTransaction(txn);
    throw CatalogException("Failed to find function " + name);
  }

  // If the language isn't 'internal', crap out ... for now ...
  auto lang_catalog_obj = proc_catalog_obj->GetLanguage();
  if (lang_catalog_obj == nullptr ||
      lang_catalog_obj->GetName() != "internal") {
    txn_manager.AbortTransaction(txn);
    throw CatalogException(
        "Peloton currently only supports internal functions. Function " + name +
        " has language '" + lang_catalog_obj->GetName() + "'");
  }

  // If the function is "internal", perform the lookup in our built-in
  // functions map (i.e., function::BuiltInFunctions) to get the function
  FunctionData result;
  result.argument_types_ = argument_types;
  result.func_name_ = proc_catalog_obj->GetSrc();
  result.return_type_ = proc_catalog_obj->GetRetType();
  result.func_ = function::BuiltInFunctions::GetFuncByName(result.func_name_);

  if (result.func_.impl == nullptr) {
    txn_manager.AbortTransaction(txn);
    throw CatalogException("Function " + name +
                           " is internal, but doesn't have a function address");
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
    if (!LanguageCatalog::GetInstance().InsertLanguage("internal", pool_.get(),
                                                       txn)) {
      txn_manager.AbortTransaction(txn);
      throw CatalogException("Failed to add language 'internal'");
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
        LanguageCatalog::GetInstance().GetLanguageByName("internal", txn);
    if (lang_object == nullptr) {
      throw CatalogException("Language 'internal' does not exist");
    }
    oid_t internal_lang = lang_object->GetOid();

    try {
      /**
       * string functions
       */
      AddBuiltinFunction(
          "ascii", {type::TypeId::VARCHAR}, type::TypeId::INTEGER,
          internal_lang, "Ascii",
          function::BuiltInFuncType{OperatorId::Ascii,
                                    function::StringFunctions::_Ascii},
          txn);
      AddBuiltinFunction("chr", {type::TypeId::INTEGER}, type::TypeId::VARCHAR,
                         internal_lang, "Chr",
                         function::BuiltInFuncType{
                             OperatorId::Chr, function::StringFunctions::Chr},
                         txn);
      AddBuiltinFunction(
          "concat", {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "Concat",
          function::BuiltInFuncType{OperatorId::Concat,
                                    function::StringFunctions::Concat},
          txn);
      AddBuiltinFunction(
          "substr",
          {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::INTEGER},
          type::TypeId::VARCHAR, internal_lang, "Substr",
          function::BuiltInFuncType{OperatorId::Substr,
                                    function::StringFunctions::Substr},
          txn);
      AddBuiltinFunction(
          "char_length", {type::TypeId::VARCHAR}, type::TypeId::INTEGER,
          internal_lang, "CharLength",
          function::BuiltInFuncType{OperatorId::CharLength,
                                    function::StringFunctions::CharLength},
          txn);
      AddBuiltinFunction(
          "octet_length", {type::TypeId::VARCHAR}, type::TypeId::INTEGER,
          internal_lang, "OctetLength",
          function::BuiltInFuncType{OperatorId::OctetLength,
                                    function::StringFunctions::OctetLength},
          txn);
      AddBuiltinFunction(
          "repeat", {type::TypeId::VARCHAR, type::TypeId::INTEGER},
          type::TypeId::VARCHAR, internal_lang, "Repeat",
          function::BuiltInFuncType{OperatorId::Repeat,
                                    function::StringFunctions::Repeat},
          txn);
      AddBuiltinFunction(
          "replace",
          {type::TypeId::VARCHAR, type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "Replace",
          function::BuiltInFuncType{OperatorId::Replace,
                                    function::StringFunctions::Replace},
          txn);
      AddBuiltinFunction(
          "ltrim", {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "LTrim",
          function::BuiltInFuncType{OperatorId::LTrim,
                                    function::StringFunctions::LTrim},
          txn);
      AddBuiltinFunction(
          "rtrim", {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "RTrim",
          function::BuiltInFuncType{OperatorId::RTrim,
                                    function::StringFunctions::RTrim},
          txn);
      AddBuiltinFunction(
          "btrim", {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "btrim",
          function::BuiltInFuncType{OperatorId::BTrim,
                                    function::StringFunctions::BTrim},
          txn);
      AddBuiltinFunction(
          "like", {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
          type::TypeId::VARCHAR, internal_lang, "like",
          function::BuiltInFuncType{OperatorId::Like,
                                    function::StringFunctions::_Like},
          txn);

      /**
       * decimal functions
       */
      AddBuiltinFunction(
          "sqrt", {type::TypeId::DECIMAL}, type::TypeId::DECIMAL, internal_lang,
          "Sqrt", function::BuiltInFuncType{OperatorId::Sqrt,
                                            function::DecimalFunctions::Sqrt},
          txn);

      /**
       * date functions
       */
      AddBuiltinFunction(
          "extract", {type::TypeId::INTEGER, type::TypeId::TIMESTAMP},
          type::TypeId::DECIMAL, internal_lang, "Extract",
          function::BuiltInFuncType{OperatorId::Extract,
                                    function::DateFunctions::Extract},
          txn);
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
