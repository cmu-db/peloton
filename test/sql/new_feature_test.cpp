//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_sql_test.cpp
//
// Identification: test/sql/aggregate_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"


namespace peloton {
  namespace test {

    class NewFeatureTest : public PelotonTest {};

    void CreateAndLoadTable() {
      // Create a table first
      LOG_INFO("Creating a table...");
      LOG_INFO(
      "Query: create table a(id int, value int)");

      TestingSQLUtil::ExecuteSQLQuery(
      "create table a(id int, value int);");

      LOG_INFO("Table created!");

      // Insert multiple tuples into table
      LOG_INFO("Inserting a tuple...");
      LOG_INFO(
              "Query: Iinsert into a values(1, 1)");
      LOG_INFO(
              "Query: Iinsert into a values(2, null)");
      LOG_INFO(
              "Query: Iinsert into a values(3, null)");
      LOG_INFO(
              "Query: Iinsert into a values(4, 4)");

      TestingSQLUtil::ExecuteSQLQuery("insert into a values(1, 1);");
      TestingSQLUtil::ExecuteSQLQuery("insert into a values(2, null);");
      TestingSQLUtil::ExecuteSQLQuery("insert into a values(3, null);");
      TestingSQLUtil::ExecuteSQLQuery("insert into a values(4, 4);");

      LOG_INFO("Tuple inserted!");
    }
      TEST_F(NewFeatureTest, WhereInTest) {
        LOG_INFO("Bootstrapping...");

        auto catalog = catalog::Catalog::GetInstance();
        catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

        LOG_INFO("Bootstrapping completed!");

        CreateAndLoadTable();

        // Update a tuple into table
        LOG_INFO("Test IN in where clause ...");
        LOG_INFO(
                "Query: select * from a where id in (1)");

        std::vector<StatementResult> result;
        std::vector<FieldInfo> tuple_descriptor;
        std::string error_message;
        int rows_affected;

        TestingSQLUtil::ExecuteSQLQuery("select value from a where id in (1);", result,
                                        tuple_descriptor, rows_affected, error_message);

        // Check the return value
        EXPECT_EQ(TestingSQLUtil::GetResultValueAsString(result, 0), "1");

        // free the database just created
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction();
        catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
        txn_manager.CommitTransaction(txn);
      }

      TEST_F(NewFeatureTest, InsertNullTest) {
        LOG_INFO("Bootstrapping...");

        auto catalog = catalog::Catalog::GetInstance();
        catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

        LOG_INFO("Bootstrapping completed!");

        CreateAndLoadTable();

        // Get insert result
        LOG_INFO("Test insert null...");
        LOG_INFO(
                "Query: select * from a");

        std::vector<StatementResult> result;
        std::vector<FieldInfo> tuple_descriptor;
        std::string error_message;
        int rows_affected;

        TestingSQLUtil::ExecuteSQLQuery("select * from a;", result,
                                        tuple_descriptor, rows_affected, error_message);
        // Check the return value
        EXPECT_EQ(result.size() / tuple_descriptor.size(), 4);
        
        // free the database just created
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction();
        catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
        txn_manager.CommitTransaction(txn);
      }

      TEST_F(NewFeatureTest, IsNullWhereTest) {
        LOG_INFO("Bootstrapping...");

        auto catalog = catalog::Catalog::GetInstance();
        catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

        LOG_INFO("Bootstrapping completed!");

        CreateAndLoadTable();

        // Get insert result
        LOG_INFO("Test is null in where clause...");
        LOG_INFO(
                "Query: select * from a where value is null");

        std::vector<StatementResult> result;
        std::vector<FieldInfo> tuple_descriptor;
        std::string error_message;
        int rows_affected;

        TestingSQLUtil::ExecuteSQLQuery("select * from a where value is null;", result,
                                        tuple_descriptor, rows_affected, error_message);
        // Check the return value
        // Should be: [2,]; [3,]
        EXPECT_EQ(result.size() / tuple_descriptor.size(), 2);
        EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 0));
        EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 1));
        EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
        EXPECT_EQ("", TestingSQLUtil::GetResultValueAsString(result, 3));
        LOG_INFO("Testing the result for is null at %s",
                 TestingSQLUtil::GetResultValueAsString(result, 1).c_str());
        // free the database just created
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction();
        catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
        txn_manager.CommitTransaction(txn);
      }

      TEST_F(NewFeatureTest, IsNotNullWhereTest) {
        LOG_INFO("Bootstrapping...");

        auto catalog = catalog::Catalog::GetInstance();
        catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

        LOG_INFO("Bootstrapping completed!");

        CreateAndLoadTable();

        // Get insert result
        LOG_INFO("Test is not null in where clause...");
        LOG_INFO(
                "Query: select * from a where value is not null");

        std::vector<StatementResult> result;
        std::vector<FieldInfo> tuple_descriptor;
        std::string error_message;
        int rows_affected;

        TestingSQLUtil::ExecuteSQLQuery("select * from a where value is not null;", result,
                                        tuple_descriptor, rows_affected, error_message);
        // Check the return value
        // Should be: [1,'hi']; [4,'bye']
        EXPECT_EQ(result.size() / tuple_descriptor.size(), 2);
        EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
        EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 1));
        EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 2));
        EXPECT_EQ("4", TestingSQLUtil::GetResultValueAsString(result, 3));

        // free the database just created
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction();
        catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
        txn_manager.CommitTransaction(txn);
      }

    }  // namespace test
}  // namespace peloton
