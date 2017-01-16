//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager_test.cpp
//
// Identification: test/gc/transaction_level_gc_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "catalog/catalog.h"
#include "common/harness.h"
#include "gc/gc_manager_factory.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/database.h"

#include "executor/executor_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionLevelGCManagerTests : public PelotonTest {};

TEST_F(TransactionLevelGCManagerTests, EnableTest) {
  gc::GCManagerFactory::Configure(1);
  EXPECT_TRUE(gc::GCManagerFactory::GetGCType() == GarbageCollectionType::ON);

  gc::GCManagerFactory::Configure(0);
  EXPECT_TRUE(gc::GCManagerFactory::GetGCType() == GarbageCollectionType::OFF);
}

TEST_F(TransactionLevelGCManagerTests, StartGC) {
  gc::GCManagerFactory::Configure(1);

  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  
  gc_manager.StartGC();
  EXPECT_TRUE(gc_manager.GetStatus() == true);

  gc_manager.StopGC();
  EXPECT_TRUE(gc_manager.GetStatus() == false);

  gc::GCManagerFactory::Configure(0);
}

TEST_F(TransactionLevelGCManagerTests, RegisterTableTest) {
  gc::GCManagerFactory::Configure(1);
  auto catalog = catalog::Catalog::GetInstance();
  // create database
  auto database = ExecutorTestsUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(catalog->HasDatabase(db_id));

  // create table
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(TESTS_TUPLES_PER_TILEGROUP, false));

  // add table into database
  int table_oid = data_table->GetOid();

  database->AddTable(data_table.get());

  EXPECT_TRUE(gc::GCManagerFactory::GetInstance().GetTableCount() == 1);

  database->DropTableWithOid(table_oid);
  
  EXPECT_TRUE(gc::GCManagerFactory::GetInstance().GetTableCount() == 0);

  data_table.release();

  // DROP!
  ExecutorTestsUtil::DeleteDatabase(DEFAULT_DB_NAME);
  EXPECT_FALSE(catalog->HasDatabase(db_id));

  gc::GCManagerFactory::Configure(0);

}


}  // End test namespace
}  // End peloton namespace

