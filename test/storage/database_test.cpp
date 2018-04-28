//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_test.cpp
//
// Identification: test/storage/database_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"

#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Database Tests
//===--------------------------------------------------------------------===//

class DatabaseTests : public PelotonTest {};

TEST_F(DatabaseTests, AddDropTest) {
  // ADD!
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // DROP!
  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->GetDatabaseObject(db_id, txn),
               CatalogException);
  txn_manager.CommitTransaction(txn);
  // Only GC will remove the actual database object
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
}

TEST_F(DatabaseTests, AddDropTableTest) {
  // ADD!
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create data table
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(TESTS_TUPLES_PER_TILEGROUP, false));

  int table_oid = data_table->GetOid();

  database->AddTable(data_table.get());
  // NOTE: everytime we create a database, there will be 8 catalog tables inside
  EXPECT_TRUE(database->GetTableCount() == 1 + CATALOG_TABLES_COUNT);

  database->DropTableWithOid(table_oid);

  EXPECT_TRUE(database->GetTableCount() == CATALOG_TABLES_COUNT);

  data_table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->GetDatabaseObject(db_id, txn),
               CatalogException);
  txn_manager.CommitTransaction(txn);
  // Only GC will remove the actual database object
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));
}

}  // namespace test
}  // namespace peloton
