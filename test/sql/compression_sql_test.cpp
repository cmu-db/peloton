//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// Compression Testing Script
//===----------------------------------------------------------------------===//

#include <memory>
#include <math.h>
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class CompressionTest : public PelotonTest {};

TEST_F(CompressionTest, IntegerTest) {
  
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year integer);");
  int i;

  for (i = 0; i < 2500; i++) {
    std::ostringstream os;
    os << "insert into foo values(" << i << ", " << i * 10 << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, 2500);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  os << "select * from foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  /*for (i = 0; i < 2500; i++) {
    std::string resultStr(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)) + "\t" +
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    std::string expectedStr(std::to_string(i) + "\t" + std::to_string(i * 10));
    EXPECT_EQ(resultStr, expectedStr);
  }*/

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CompressionTest, DecimalTest) {
  
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year decimal);");
  int i;
  for (i = 0; i < 2500; i++) {
    std::ostringstream os;
    float value = (float) i;
    float actual_year = value + (value + 5.0) / 10.0;
    std::stringstream stream;
    stream << std::fixed << std::setprecision(2) << actual_year;
    std::string inserted_value = stream.str();
    os << "insert into foo values(" << i << ", " << inserted_value << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  EXPECT_EQ(i, 2500);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  os << "select * from foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < 2500; i++) {
    std::string result_id(TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_year(TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    float year = std::stof(result_year);
    float value = (float) i;
    float actual_year = value + (value + 5.0) / 10.0;
    EXPECT_EQ(id, i);
    EXPECT_EQ(year, actual_year);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}
}
