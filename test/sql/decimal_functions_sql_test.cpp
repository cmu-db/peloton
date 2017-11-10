//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions_sql_test.cpp
//
// Identification: test/sql/decimal_functions_sql_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class DecimalFunctionsSQLTest : public PelotonTest {};

TEST_F(DecimalFunctionsSQLTest, AbsTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, income decimal);");
  // Adding in 2500 random decimal inputs between [-500, 500]
  int i;
  std::vector<double> inputs;
  int lo = -500;
  int hi = 500;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    double num = 0.45 + (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::string sql_line = "insert into foo values(" + std::to_string(i) +
                           ", " + std::to_string(num) + ");";
    TestingSQLUtil::ExecuteSQLQuery(sql_line);
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string sql_line = "select id, abs(income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(sql_line, result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double income = std::stod(result_income);
    EXPECT_EQ(id, i);
    EXPECT_DOUBLE_EQ(income, fabs(inputs[i]));
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton