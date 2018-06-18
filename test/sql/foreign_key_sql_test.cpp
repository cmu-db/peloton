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

TEST_F(ForeignKeySQLTests, NoActionTest) {

  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(txn, DEFAULT_DB_NAME);
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

  // Update/delete any referenced row should fail
  EXPECT_NE(TestingSQLUtil::ExecuteSQLQuery(
            "UPDATE tb1 SET id = 10 where id = 1;"), ResultType::SUCCESS);

  EXPECT_NE(TestingSQLUtil::ExecuteSQLQuery(
            "DELETE FROM tb1 WHERE id = 1;"), ResultType::SUCCESS);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

TEST_F(ForeignKeySQLTests, CascadeTest) {
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE tb1(id INT PRIMARY KEY);");

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE tb2(num INT REFERENCES tb1(id) on update cascade on delete cascade);");

  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO tb1 VALUES (1);"), ResultType::SUCCESS);

  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO tb2 VALUES (1);"), ResultType::SUCCESS);

  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery(
      "UPDATE tb1 SET id = 10 where id = 1;"), ResultType::SUCCESS);

  TestingSQLUtil::ExecuteSQLQueryAndCheckResult("SELECT id from tb1;",
                                                {"10"},
                                                true);

  TestingSQLUtil::ExecuteSQLQueryAndCheckResult("SELECT num from tb2;",
                                                {"10"},
                                                true);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}

}
}