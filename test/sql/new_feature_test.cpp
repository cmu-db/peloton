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
        TEST_F(NewFeatureTest, WhereInTest) {
          LOG_INFO("Bootstrapping...");

          auto catalog = catalog::Catalog::GetInstance();
          catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

          LOG_INFO("Bootstrapping completed!");

          // Create a table first
          LOG_INFO("Creating a table...");
          LOG_INFO(
                  "Query: create table a(id int, value varchar)");

          TestingSQLUtil::ExecuteSQLQuery(
                  "create table a(id int, value varchar);");

          LOG_INFO("Table created!");

          // Insert a tuple into table
          LOG_INFO("Inserting a tuple...");
          LOG_INFO(
                  "Query: Iinsert into a values(1, 'hi')");

          TestingSQLUtil::ExecuteSQLQuery("insert into a values(1, 'hi');");

          LOG_INFO("Tuple inserted!");

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
          EXPECT_EQ(result[0].second[0], 'hi');

          // free the database just created
          auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
          auto txn = txn_manager.BeginTransaction();
          catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
          txn_manager.CommitTransaction(txn);
        }

    }  // namespace test
}  // namespace peloton
