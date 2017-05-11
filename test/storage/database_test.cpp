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

#include "catalog/catalog.h"
#include "common/harness.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/database.h"

#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Database Tests
//===--------------------------------------------------------------------===//

class DatabaseTests : public PelotonTest {};

TEST_F(DatabaseTests, AddDropTest) {
  // ADD!
  auto catalog = catalog::Catalog::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(catalog->HasDatabase(db_id));

  // DROP!
  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
  EXPECT_FALSE(catalog->HasDatabase(db_id));
}


TEST_F(DatabaseTests, AddDropTableTest) {
  // ADD!
  auto catalog = catalog::Catalog::GetInstance();
  auto database = TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(catalog->HasDatabase(db_id));

  // create data table
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(TESTS_TUPLES_PER_TILEGROUP, false));

  int table_oid = data_table->GetOid();

  database->AddTable(data_table.get());

  EXPECT_TRUE(database->GetTableCount() == 1);


  database->DropTableWithOid(table_oid);
  
  EXPECT_TRUE(database->GetTableCount() == 0);

  data_table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
  EXPECT_FALSE(catalog->HasDatabase(db_id));
}

}  // End test namespace
}  // End peloton namespace
