//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_test.cpp
//
// Identification: test/executor/drop_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "executor/drop_executor.h"
#include "planner/drop_plan.h"
#include "planner/plan_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DropTests : public PelotonTest {};

TEST_F(DropTests, DroppingTable) {
  auto catalog = catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(type::TypeId::INTEGER,
                                   type::Type::GetTypeSize(type::TypeId::INTEGER),
                                   "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema2(
      new catalog::Schema({id_column, name_column}));

  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(DEFAULT_DB_NAME, "department_table",
                       std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(DEFAULT_DB_NAME, "department_table_2",
                       std::move(table_schema2), txn);
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 2);

  // Now dropping the table using the executer
  txn = txn_manager.BeginTransaction();
  catalog->DropTable(DEFAULT_DB_NAME, "department_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME)->GetTableCount(), 1);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
