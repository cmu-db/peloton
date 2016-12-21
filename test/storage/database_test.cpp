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
#include "executor/executor_tests_util.h"
#include "storage/database.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Database Tests
//===--------------------------------------------------------------------===//

class DatabaseTests : public PelotonTest {};

TEST_F(DatabaseTests, AddDropTest) {
  // ADD!
  auto catalog = catalog::Catalog::GetInstance();
  auto database = ExecutorTestsUtil::InitializeDatabase(DEFAULT_DB_NAME);
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(catalog->HasDatabase(db_id));

  // DROP!
  ExecutorTestsUtil::DeleteDatabase(DEFAULT_DB_NAME);
  EXPECT_FALSE(catalog->HasDatabase(db_id));
}

}  // End test namespace
}  // End peloton namespace
