//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// is_null_sql_test.cpp
//
// Identification: test/sql/is_null_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/testing_transaction_util.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class IsNullSqlTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  LOG_TRACE("Creating a table...");
  LOG_TRACE("Query: create table a(id int, value int)");

  TestingSQLUtil::ExecuteSQLQuery("create table a(id int, value int);");

  LOG_TRACE("Table created!");

  // Insert multiple tuples into table
  LOG_TRACE("Inserting a tuple...");
  LOG_TRACE("Query: insert into a values(1, 1)");
  LOG_TRACE("Query: insert into a values(2, null)");
  LOG_TRACE("Query: insert into a values(3, null)");
  LOG_TRACE("Query: insert into a values(4, 4)");

  TestingSQLUtil::ExecuteSQLQuery("insert into a values(1, 1);");
  TestingSQLUtil::ExecuteSQLQuery("insert into a values(2, null);");
  TestingSQLUtil::ExecuteSQLQuery("insert into a values(3, null);");
  TestingSQLUtil::ExecuteSQLQuery("insert into a values(4, 4);");

  LOG_TRACE("Tuple inserted!");
}

TEST_F(IsNullSqlTests, InsertNullTest) {
  LOG_TRACE("Bootstrapping...");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_TRACE("Bootstrapping completed!");

  CreateAndLoadTable();

  // Get insert result
  LOG_TRACE("Test insert null...");
  LOG_TRACE("Query: select * from a");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("select * from a;", result, tuple_descriptor,
                                  rows_affected, error_message);
  // Check the return value
  EXPECT_EQ(result.size() / tuple_descriptor.size(), 4);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(IsNullSqlTests, IsNullWhereTest) {
  LOG_TRACE("Bootstrapping...");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_TRACE("Bootstrapping completed!");

  CreateAndLoadTable();

  // Get insert result
  LOG_TRACE("Test is null in where clause...");
  LOG_TRACE("Query: select * from a where value is null");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("select * from a where value is null;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  // Should be: [2,]; [3,]
  EXPECT_EQ(result.size() / tuple_descriptor.size(), 2);
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 3));
  LOG_TRACE("Testing the result for is null at %s",
            TestingSQLUtil::GetResultValueAsString(result, 1).c_str());
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(IsNullSqlTests, IsNotNullWhereTest) {
  LOG_TRACE("Bootstrapping...");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  LOG_TRACE("Bootstrapping completed!");

  CreateAndLoadTable();

  // Get insert result
  LOG_TRACE("Test is not null in where clause...");
  LOG_TRACE("Query: select * from a where value is not null");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery("select * from a where value is not null;",
                                  result, tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  // Should be: [1,'hi']; [4,'bye']
  EXPECT_EQ(result.size() / tuple_descriptor.size(), 2);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 3));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton