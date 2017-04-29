//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// Compression Testing Script
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

#define WORDNUM 4
namespace peloton {
namespace test {

class CompressionDictionaryTest : public PelotonTest {};

TEST_F(CompressionDictionaryTest, BasicTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year integer, word VARCHAR(20) );");
  int i;

  int j = 0;
  std::string word = "";

  for (i = 0; i < 2500; i++) {
    switch (j) {
      case 0:
        word = "foo";
        break;
      case 1:
        word = "bar";
        break;
      case 2:
        word = "hello";
        break;
      case 3:
        word = "world";
        break;
      default:
        word = "null";
        break;
    }
    j++;
    if (j >= WORDNUM) j = 0;

    std::ostringstream os;
    os << "insert into foo values(" << i << ", " << i * 10 << ", '" << word
       << "' );";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "foo");
  EXPECT_EQ(i, 2500);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  storage::DataTable *foo_table =
      catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "foo");
  foo_table->CompressTable();
  // os << "select * from foo;";
  os << "select id,year  from foo;";

  // os << "select word from foo;";
  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);

  std::ostringstream os2;
  os2 << "select word from foo;";
  TestingSQLUtil::ExecuteSQLQuery(os2.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < 10; i++) {
    LOG_INFO("%s", TestingSQLUtil::GetResultValueAsString(result, i).c_str());
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CompressionDictionaryTest, UniqTest) {
  // every varchar is different in this case
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year integer, word VARCHAR(20) );");
  int i;

  for (i = 0; i < 2500; i++) {
    std::ostringstream os;
    os << "insert into foo values(" << i << ", " << i * 10 << ", '"
       << std::to_string(2500 - i) << "' );";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "foo");
  EXPECT_EQ(i, 2500);

  storage::DataTable *foo_table =
      catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "foo");
  foo_table->CompressTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  // os << "select * from foo;";
  os << "select id,year  from foo;";

  // os << "select word from foo;";
  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);

  std::ostringstream os2;
  os2 << "select word from foo;";
  TestingSQLUtil::ExecuteSQLQuery(os2.str(), result, tuple_descriptor,
                                  rows_affected, error_message);

  for (i = 0; i < 2500; i++) {
    std::string resultStr(TestingSQLUtil::GetResultValueAsString(result, i));
    std::string expectedStr(std::to_string(2500 - i));
    EXPECT_EQ(resultStr, expectedStr);
  }
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CompressionDictionaryTest, SizeTest) {
  int uniqs[] = {128, 129, 255, 600};
  for (int j = 0; j < 4; j++) {
    // the number of unique  var may vary

    // 1 ~ 128 : tiny
    // 128~more  small int
    int uniq = uniqs[j];

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

    txn_manager.CommitTransaction(txn);
    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE foo(id integer, year integer, word VARCHAR(20) );");
    int i;

    for (i = 0; i < 2500; i++) {
      std::ostringstream os;
      os << "insert into foo values(" << i << ", " << i * 10 << ", '"
         << std::to_string((2500 - i) % uniq) << "' );";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
    }
    TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "foo");
    EXPECT_EQ(i, 2500);

    storage::DataTable *foo_table =
        catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                          "foo");
    foo_table->CompressTable();

    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected;

    std::ostringstream os2;
    os2 << "select word from foo;";
    TestingSQLUtil::ExecuteSQLQuery(os2.str(), result, tuple_descriptor,
                                    rows_affected, error_message);
    for (i = 0; i < 2500; i++) {
      std::string resultStr(TestingSQLUtil::GetResultValueAsString(result, i));
      std::string expectedStr(std::to_string((2500 - i) % uniq));
      EXPECT_EQ(resultStr, expectedStr);
    }

    // free the database just created
    txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }
}
}
}
