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
//===----------------------------------------------------------------------===//
#include "catalog/catalog.h"

#include <iostream>

#include "catalog/database_metrics_catalog.h"
#include "catalog/manager.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "expression/date_functions.h"
#include "expression/string_functions.h"
#include "expression/decimal_functions.h"
#include "index/index_factory.h"
#include "util/string_util.h"

namespace peloton {
namespace catalog {

// Get instance of the global catalog
Catalog *Catalog::GetInstance(void) {
  static std::unique_ptr<Catalog> global_catalog(new Catalog());
  return global_catalog.get();
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

  // Create pg_catalog database
  auto pg_catalog = new storage::Database(CATALOG_DATABASE_OID);
  pg_catalog->setDBName(CATALOG_DATABASE_NAME);
  databases_.push_back(pg_catalog);

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
              std::vector<std::string>({"database_name"}),
              DATABASE_CATALOG_NAME "_skey0", IndexType::BWTREE,
              IndexConstraintType::UNIQUE, true, txn, true);

  CreateIndex(CATALOG_DATABASE_OID, TABLE_CATALOG_OID,
              std::vector<std::string>({"table_name", "database_oid"}),
              TABLE_CATALOG_NAME "_skey0", IndexType::BWTREE,
              IndexConstraintType::UNIQUE, true, txn, true);
  CreateIndex(CATALOG_DATABASE_OID, TABLE_CATALOG_OID,
              std::vector<std::string>({"database_oid"}),
              TABLE_CATALOG_NAME "_skey1", IndexType::BWTREE,
              IndexConstraintType::DEFAULT, false, txn, true);

  // actual index already added in column_catalog, index_catalog constructor
  // the reason we treat those two catalog tables differently is that indexes
  // needs to be built before insert tuples into table
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey", COLUMN_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, true,
      std::vector<oid_t>({0, 1}), pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_SKEY0_OID, COLUMN_CATALOG_NAME "_skey0",
      COLUMN_CATALOG_OID, IndexType::BWTREE, IndexConstraintType::UNIQUE, true,
      std::vector<oid_t>({0, 2}), pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      COLUMN_CATALOG_SKEY1_OID, COLUMN_CATALOG_NAME "_skey1",
      COLUMN_CATALOG_OID, IndexType::BWTREE, IndexConstraintType::DEFAULT,
      false, std::vector<oid_t>({0}), pool_.get(), txn);

  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_PKEY_OID, INDEX_CATALOG_NAME "_pkey", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, true,
      std::vector<oid_t>({0}), pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_SKEY0_OID, INDEX_CATALOG_NAME "_skey0", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::UNIQUE, true,
      std::vector<oid_t>({1}), pool_.get(), txn);
  IndexCatalog::GetInstance()->InsertIndex(
      INDEX_CATALOG_SKEY1_OID, INDEX_CATALOG_NAME "_skey1", INDEX_CATALOG_OID,
      IndexType::BWTREE, IndexConstraintType::DEFAULT, false,
      std::vector<oid_t>({2}), pool_.get(), txn);

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

  InitializeFunctions();
}

void Catalog::Bootstrap() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  DatabaseMetricsCatalog::GetInstance(txn);
  TableMetricsCatalog::GetInstance(txn);
  IndexMetricsCatalog::GetInstance(txn);
  QueryMetricsCatalog::GetInstance(txn);

  txn_manager.CommitTransaction(txn);
}

//===----------------------------------------------------------------------===//
// CREATE FUNCTIONS
//===----------------------------------------------------------------------===//

ResultType Catalog::CreateDatabase(const std::string &database_name,
                                   concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create database: %s",
              database_name.c_str());
    return ResultType::FAILURE;
  }

  auto pg_database = DatabaseCatalog::GetInstance();
  // Check if a database with the same name exists
  oid_t database_oid = pg_database->GetDatabaseOid(database_name, txn);
  if (database_oid != INVALID_OID) {
    LOG_TRACE("Database  %s already exists.", database_name.c_str());
    return ResultType::FAILURE;
  }

  // Create actual database
  database_oid = pg_database->GetNextOid();

  storage::Database *database = new storage::Database(database_oid);

  // TODO: This should be deprecated, dbname should only exists in pg_db
  database->setDBName(database_name);

  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    databases_.push_back(database);
  }

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
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create table: %s",
              table_name.c_str());
    return ResultType::FAILURE;
  }

  LOG_TRACE("Creating table %s in database %s", table_name.c_str(),
            database_name.c_str());
  // get database oid from pg_database
  oid_t database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);
  if (database_oid == INVALID_OID) {
    LOG_TRACE("Cannot find the database %s in pg_db", database_name.c_str());
    return ResultType::FAILURE;
  }

  // get table oid from pg_table
  oid_t table_oid =
      TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, txn);
  if (table_oid != INVALID_OID) {
    LOG_TRACE("Cannot find the table %s in pg_table", table_name.c_str());
    return ResultType::FAILURE;
  }

  try {
    auto database = GetDatabaseWithOid(database_oid);

    // Check duplicate column names
    std::set<std::string> column_names;
    auto columns = schema.get()->GetColumns();

    for (auto column : columns) {
      auto column_name = column.GetName();
      if (column_names.count(column_name) == 1) {
        LOG_TRACE(
            "Can't create table %s with duplicate column names. RESULT_FAILURE",
            table_name.c_str());
        return ResultType::FAILURE;
      }
      column_names.insert(column_name);
    }

    // Create actual table
    auto pg_table = TableCatalog::GetInstance();
    table_oid = pg_table->GetNextOid();
    bool own_schema = true;
    bool adapt_table = false;
    auto table = storage::TableFactory::GetDataTable(
        database_oid, table_oid, schema.release(), table_name,
        DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table, is_catalog);
    database->AddTable(table, is_catalog);

    // Update pg_table with table info
    pg_table->InsertTable(table_oid, table_name, database_oid, pool_.get(),
                          txn);
    oid_t column_id = 0;
    for (auto column : table->GetSchema()->GetColumns()) {
      ColumnCatalog::GetInstance()->InsertColumn(
          table_oid, column.GetName(), column_id, column.GetOffset(),
          column.GetType(), column.IsInlined(), column.GetConstraints(),
          pool_.get(), txn);
      column_id++;
    }

    CreatePrimaryIndex(database_oid, table_oid, txn);

    return ResultType::SUCCESS;
  } catch (CatalogException &e) {
    LOG_TRACE("Can't found database %s. Return RESULT_FAILURE",
              database_name.c_str());
    return ResultType::FAILURE;
  }
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
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create table: %d", (int)table_oid);
    return ResultType::FAILURE;
  }

  LOG_TRACE("Trying to create primary index for table %d", table_oid);

  try {
    auto database = GetDatabaseWithOid(database_oid);
    try {
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
          index_name, index_oid, table->GetOid(), database->GetOid(),
          IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, schema,
          key_schema, key_attrs, unique_keys);

      std::shared_ptr<index::Index> pkey_index(
          index::IndexFactory::GetIndex(index_metadata));
      table->AddIndex(pkey_index);

      // insert index record into index_catalog(pg_index) table
      IndexCatalog::GetInstance()->InsertIndex(
          index_oid, index_name, table->GetOid(), IndexType::BWTREE,
          IndexConstraintType::PRIMARY_KEY, unique_keys, key_attrs, pool_.get(),
          txn);

      LOG_TRACE("Successfully created primary key index '%s' for table '%s'",
                pkey_index->GetName().c_str(), table->GetName().c_str());

      return ResultType::SUCCESS;
    } catch (CatalogException &e) {
      LOG_TRACE(
          "Cannot find the table %d to create the primary key index. Return "
          "RESULT_FAILURE.",
          table_oid);
      return ResultType::FAILURE;
    }
  } catch (CatalogException &e) {
    LOG_TRACE("Could not find a database with oid %d", database_oid);
    return ResultType::FAILURE;
  }
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
                                const std::vector<std::string> &index_attr,
                                const std::string &index_name, bool unique_keys,
                                IndexType index_type,
                                concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create index: %s",
              index_name.c_str());
    return ResultType::FAILURE;
  }

  LOG_TRACE("Trying to create index %s for table %s", index_name.c_str(),
            table_name.c_str());

  oid_t database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);
  if (database_oid == INVALID_OID) {
    LOG_TRACE(
        "Cannot find the database to create the primary key index. Return "
        "RESULT_FAILURE.");
    return ResultType::FAILURE;
  }

  oid_t table_oid =
      TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, txn);
  if (table_oid == INVALID_OID) {
    LOG_TRACE(
        "Cannot find the table %s to create index. Return "
        "RESULT_FAILURE.",
        table_name.c_str());
    return ResultType::FAILURE;
  }

  IndexConstraintType index_constraint =
      unique_keys ? IndexConstraintType::UNIQUE : IndexConstraintType::DEFAULT;

  ResultType success =
      CreateIndex(database_oid, table_oid, index_attr, index_name, index_type,
                  index_constraint, unique_keys, txn);

  return success;
}

ResultType Catalog::CreateIndex(oid_t database_oid, oid_t table_oid,
                                const std::vector<std::string> &index_attr,
                                const std::string &index_name,
                                IndexType index_type,
                                IndexConstraintType index_constraint,
                                bool unique_keys, concurrency::Transaction *txn,
                                bool is_catalog) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create index: %s",
              index_name.c_str());
    return ResultType::FAILURE;
  }

  LOG_TRACE("Trying to create index for table %d", table_oid);

  // check if table already has index with same name
  auto pg_index = IndexCatalog::GetInstance();
  if (!is_catalog && pg_index->GetIndexOid(index_name, txn) != INVALID_OID) {
    LOG_TRACE(
        "Cannot create index with same name Return "
        "RESULT_FAILURE.");
    return ResultType::FAILURE;
  }

  try {
    auto database = GetDatabaseWithOid(database_oid);
    try {
      auto table = database->GetTableWithOid(table_oid);

      // check if index attributes are in table, and get indexed column ids
      std::vector<oid_t> key_attrs;
      auto schema = table->GetSchema();
      auto &columns = schema->GetColumns();
      for (auto &attr : index_attr) {
        // TODO: Shall we use pg_attribute to check column_ids instead?
        for (oid_t i = 0; i < columns.size(); ++i) {
          if (attr == columns[i].column_name) {
            key_attrs.push_back(i);
            break;
          }
        }
      }

      // Check for mismatch between key attributes and attributes
      // that came out of the parser
      if (key_attrs.size() != index_attr.size()) {
        LOG_INFO("Some columns are missing");
        return ResultType::FAILURE;
      }

      // Passed all checks, now get all index metadata
      LOG_TRACE("Trying to create index %s on table %d", index_name.c_str(),
                table_oid);
      oid_t index_oid = pg_index->GetNextOid();
      auto key_schema = catalog::Schema::CopySchema(schema, key_attrs);
      key_schema->SetIndexedColumns(key_attrs);

      // Set index metadata
      auto index_metadata = new index::IndexMetadata(
          index_name, index_oid, table->GetOid(), database->GetOid(),
          index_type, index_constraint, schema, key_schema, key_attrs,
          unique_keys);

      // Add index to table
      std::shared_ptr<index::Index> key_index(
          index::IndexFactory::GetIndex(index_metadata));
      table->AddIndex(key_index);

      // Insert index record into pg_index
      IndexCatalog::GetInstance()->InsertIndex(
          index_oid, index_name, table_oid, index_type, index_constraint,
          unique_keys, key_attrs, pool_.get(), txn);

      LOG_TRACE("Successfully add index for table %s contains %d indexes",
                table->GetName().c_str(), (int)table->GetValidIndexCount());

      return ResultType::SUCCESS;
    } catch (CatalogException &e) {
      LOG_TRACE(
          "Cannot find the table %d to create the index. Return "
          "RESULT_FAILURE.",
          table_oid);
      return ResultType::FAILURE;
    }
  } catch (CatalogException &e) {
    LOG_TRACE(
        "Cannot find the database %d to create the index. Return "
        "RESULT_FAILURE.",
        database_oid);
    return ResultType::FAILURE;
  }
}

//===----------------------------------------------------------------------===//
// DROP FUNCTIONS
//===----------------------------------------------------------------------===//

/*
* only for test purposes
*/
ResultType Catalog::DropDatabaseWithName(const std::string &database_name,
                                         concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create database: %s",
              database_name.c_str());
    return ResultType::FAILURE;
  }

  oid_t database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);
  if (database_oid == INVALID_OID) {
    LOG_TRACE("Database %s is not found!", database_name.c_str());
    return ResultType::FAILURE;
  }

  return DropDatabaseWithOid(database_oid, txn);
}

ResultType Catalog::DropDatabaseWithOid(oid_t database_oid,
                                        concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create database: %d",
              (int)database_oid);
    return ResultType::FAILURE;
  }

  // Drop actual tables in the database
  auto table_oids =
      TableCatalog::GetInstance()->GetTableOids(database_oid, txn);
  for (auto table_oid : table_oids) {
    DropTable(database_oid, table_oid, txn);
  }

  // Drop database record in catalog
  LOG_TRACE("Deleting tuple from pg_db");
  if (!DatabaseCatalog::GetInstance()->DeleteDatabase(database_oid, txn)) {
    LOG_TRACE("Database tuple is not found in pg_db!");
    return ResultType::FAILURE;
  }

  // Drop actual database object
  LOG_TRACE("Dropping database with oid: %d", database_oid);
  bool found_database = false;
  std::lock_guard<std::mutex> lock(catalog_mutex);
  for (auto it = databases_.begin(); it != databases_.end(); ++it) {
    if ((*it)->GetOid() == database_oid) {
      LOG_TRACE("Deleting database object in database vector");
      delete (*it);
      databases_.erase(it);
      found_database = true;
      break;
    }
  }
  if (!found_database) {
    LOG_TRACE("Database %d is not found!", database_oid);
    return ResultType::FAILURE;
  }
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
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to drop table: %s", table_name.c_str());
    return ResultType::FAILURE;
  }

  // Checking if statement is valid
  oid_t database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);
  if (database_oid == INVALID_OID) {
    LOG_TRACE("Cannot find database  %s!", database_name.c_str());
    return ResultType::FAILURE;
  }

  oid_t table_oid =
      TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, txn);
  if (table_oid == INVALID_OID) {
    LOG_TRACE("Cannot find Table %s to drop!", table_name.c_str());
    return ResultType::FAILURE;
  }
  ResultType result = DropTable(database_oid, table_oid, txn);

  return result;
}

ResultType Catalog::DropTable(oid_t database_oid, oid_t table_oid,
                              concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to create table: %d", (int)table_oid);
    return ResultType::FAILURE;
  }

  LOG_TRACE("Dropping table %d from database %d", database_oid, table_oid);

  try {
    auto database = GetDatabaseWithOid(database_oid);
    // auto table = database->GetTableWithOid(table_oid);
    LOG_TRACE("Deleting table!");
    // STEP 1, read index_oids from pg_index, and iterate through
    auto index_oids = IndexCatalog::GetInstance()->GetIndexOids(table_oid, txn);
    LOG_TRACE("dropping #%d indexes", (int)index_oids.size());

    for (oid_t index_oid : index_oids) DropIndex(index_oid, txn);
    // STEP 2
    ColumnCatalog::GetInstance()->DeleteColumns(table_oid, txn);
    // STEP 3
    TableCatalog::GetInstance()->DeleteTable(table_oid, txn);
    // STEP 4
    database->DropTableWithOid(table_oid);

    return ResultType::SUCCESS;
  } catch (CatalogException &e) {
    LOG_TRACE("Can't find database %d! Return RESULT_FAILURE", database_oid);
    return ResultType::FAILURE;
  }
}

/*@brief   Drop Index on table
* @param   index_oid      the oid of the index to be dropped
* @param   txn            Transaction
* @return  Transaction ResultType(SUCCESS or FAILURE)
*/
ResultType Catalog::DropIndex(oid_t index_oid, concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to drop index: %d", (int)index_oid);
    return ResultType::FAILURE;
  }

  // find table_oid by looking up pg_index using index_oid
  // txn is nullptr, one sentence Transaction
  oid_t table_oid = IndexCatalog::GetInstance()->GetTableOid(index_oid, txn);
  if (table_oid == INVALID_OID) {
    LOG_TRACE("Cannot find the table to drop index. Return RESULT_FAILURE.");
    return ResultType::FAILURE;
  }

  // find database_oid by looking up pg_table using table_oid
  // txn is nullptr, one sentence Transaction
  oid_t database_oid =
      TableCatalog::GetInstance()->GetDatabaseOid(table_oid, txn);

  try {
    auto database = GetDatabaseWithOid(database_oid);
    try {
      auto table = database->GetTableWithOid(table_oid);
      // drop index in actual table
      table->DropIndexWithOid(index_oid);

      // drop record in pg_index
      IndexCatalog::GetInstance()->DeleteIndex(index_oid, txn);

      LOG_TRACE("Successfully drop index %d for table %s", index_oid,
                table->GetName().c_str());

      return ResultType::SUCCESS;
    } catch (CatalogException &e) {
      LOG_TRACE(
          "Can't find the table %d to drop the index. Return RESULT_FAILURE.",
          (int)table_oid);
      return ResultType::FAILURE;
    }
  } catch (CatalogException &e) {
    LOG_TRACE(
        "Can't found database %d to drop the index. Return RESULT_FAILURE",
        (int)database_oid);
    return ResultType::FAILURE;
  }
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
  // FIXME: enforce caller to use txn
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;
  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  // Check in pg_database using txn
  oid_t database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);

  if (database_oid == INVALID_OID) {
    txn_manager.AbortTransaction(txn);  // Implicitly abort txn
    throw CatalogException("Database " + database_name + " is not found");
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return GetDatabaseWithOid(database_oid);
}

/* Check table from pg_table with table_name using txn,
 * get it from storage layer using table_oid,
 * throw exception and abort txn if not exists/invisible
 * */
storage::DataTable *Catalog::GetTableWithName(const std::string &database_name,
                                              const std::string &table_name,
                                              concurrency::Transaction *txn) {
  // FIXME: enforce caller to use txn
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;
  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  LOG_TRACE("Looking for table %s in database %s", table_name.c_str(),
            database_name.c_str());

  // Check in pg_database, throw exception and abort txn if not exists
  auto database_oid =
      DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);

  if (database_oid == INVALID_OID) {
    txn_manager.AbortTransaction(txn);  // Implicitly abort txn
    throw CatalogException("Database " + database_name + " is not found");
  }

  // Check in pg_table using txn
  auto table_oid =
      TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, txn);

  if (table_oid == INVALID_OID) {
    //    txn_manager.AbortTransaction(txn);  // Implicitly abort txn
    throw CatalogException("Table " + table_name + " is not found");
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return GetTableWithOid(database_oid, table_oid);
}

//===--------------------------------------------------------------------===//
// GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
//===--------------------------------------------------------------------===//

/* Find a database using its oid from storage layer,
 * throw exception if not exists
 * */
storage::Database *Catalog::GetDatabaseWithOid(oid_t database_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == database_oid) return database;
  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
                         " is not found");
  return nullptr;
}

/* Find a table using its oid from storage layer,
 * throw exception if not exists
 * */
storage::DataTable *Catalog::GetTableWithOid(oid_t database_oid,
                                             oid_t table_oid) const {
  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
            table_oid);
  // Lookup DB from storage layer
  auto database =
      GetDatabaseWithOid(database_oid);  // Throw exception if not exists
  // Lookup table from storage layer
  return database->GetTableWithOid(table_oid);  // Throw exception if not exists
}

/* Find a index using its oid from storage layer,
 * throw exception if not exists
 * */
index::Index *Catalog::GetIndexWithOid(oid_t database_oid, oid_t table_oid,
                                       oid_t index_oid) const {
  // Lookup table from storage layer
  auto table = GetTableWithOid(database_oid,
                               table_oid);  // Throw exception if not exists
  // Lookup index from storage layer
  return table->GetIndexWithOid(index_oid)
      .get();  // Throw exception if not exists
}

//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

// Only used for testing
bool Catalog::HasDatabase(oid_t db_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == db_oid) return (true);
  return (false);
}

oid_t Catalog::GetDatabaseCount() { return databases_.size(); }

Catalog::~Catalog() {
  LOG_TRACE("Deleting databases");
  for (auto database : databases_) delete database;
  LOG_TRACE("Finish deleting database");
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This should be deprecated! this can screw up the database oid system
void Catalog::AddDatabase(storage::Database *database) {
  std::lock_guard<std::mutex> lock(catalog_mutex);
  databases_.push_back(database);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  DatabaseCatalog::GetInstance()->InsertDatabase(
      database->GetOid(), database->GetDBName(), pool_.get(),
      txn);  // I guess this can pass tests
  txn_manager.CommitTransaction(txn);
}

// This is used as an iterator
storage::Database *Catalog::GetDatabaseWithOffset(oid_t database_offset) const {
  PL_ASSERT(database_offset < databases_.size());
  auto database = databases_.at(database_offset);
  return database;
}

//===--------------------------------------------------------------------===//
// FUNCTION
//===--------------------------------------------------------------------===//

void Catalog::AddFunction(
    const std::string &name,
    const std::vector<type::Type::TypeId> &argument_types,
    const type::Type::TypeId return_type,
    type::Value (*func_ptr)(const std::vector<type::Value> &)) {
  PL_ASSERT(functions_.count(name) == 0);
  functions_[name] = FunctionData{name, argument_types, return_type, func_ptr};
}

const FunctionData Catalog::GetFunction(const std::string &name) {
  if (functions_.count(name) == 0) {
    throw Exception("function " + name + " not found.");
  }
  return functions_[name];
}

void Catalog::RemoveFunction(const std::string &name) {
  functions_.erase(name);
}

void Catalog::InitializeFunctions() {
  /**
   * string functions
   */
  AddFunction("ascii", {type::Type::VARCHAR}, type::Type::INTEGER,
              expression::StringFunctions::Ascii);
  AddFunction("chr", {type::Type::INTEGER}, type::Type::VARCHAR,
              expression::StringFunctions::Chr);
  AddFunction("substr",
              {type::Type::VARCHAR, type::Type::INTEGER, type::Type::INTEGER},
              type::Type::VARCHAR, expression::StringFunctions::Substr);
  AddFunction("concat", {type::Type::VARCHAR, type::Type::VARCHAR},
              type::Type::VARCHAR, expression::StringFunctions::Concat);
  AddFunction("char_length", {type::Type::VARCHAR}, type::Type::INTEGER,
              expression::StringFunctions::CharLength);
  AddFunction("octet_length", {type::Type::VARCHAR}, type::Type::INTEGER,
              expression::StringFunctions::OctetLength);
  AddFunction("repeat", {type::Type::VARCHAR, type::Type::INTEGER},
              type::Type::VARCHAR, expression::StringFunctions::Repeat);
  AddFunction("replace",
              {type::Type::VARCHAR, type::Type::VARCHAR, type::Type::VARCHAR},
              type::Type::VARCHAR, expression::StringFunctions::Replace);
  AddFunction("ltrim", {type::Type::VARCHAR, type::Type::VARCHAR},
              type::Type::VARCHAR, expression::StringFunctions::LTrim);
  AddFunction("rtrim", {type::Type::VARCHAR, type::Type::VARCHAR},
              type::Type::VARCHAR, expression::StringFunctions::RTrim);
  AddFunction("btrim", {type::Type::VARCHAR, type::Type::VARCHAR},
              type::Type::VARCHAR, expression::StringFunctions::BTrim);
  AddFunction("sqrt", {type::Type::DECIMAL}, type::Type::DECIMAL,
              expression::DecimalFunctions::Sqrt);

  /**
   * date functions
   */
  AddFunction("extract", {type::Type::INTEGER, type::Type::TIMESTAMP},
              type::Type::DECIMAL, expression::DateFunctions::Extract);
}
}
}
