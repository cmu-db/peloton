#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class ForeignKeySQLTests : public PelotonTest {};

TEST_F(ForeignKeySQLTests, SimpleTest) {

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE tb1(id INT PRIMARY KEY);");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE tb2(num INT REFERENCES tb1(id));");

  EXPECT_NE(TestingSQLUtil::ExecuteSQLQuery(
            "INSERT INTO tb2 VALUES (1);"), ResultType::SUCCESS);

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO tb1 VALUES (1);");

  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery(
            "INSERT INTO tb2 VALUES (1);"), ResultType::SUCCESS);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}
}