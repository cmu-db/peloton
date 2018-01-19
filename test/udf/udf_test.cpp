//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_test.cpp
//
// Identification: test/udf/udf_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class UDFTest : public PelotonTest {};

TEST_F(UDFTest, SimpleExpressionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE OR REPLACE FUNCTION increment(i double)"
      " RETURNS double AS $$ BEGIN RETURN i + 1; END;"
      " $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(income double);");
  // Adding in 2500 random decimal inputs between [-500, 500]
  int i;
  std::vector<double> inputs;
  int lo = -500;
  int hi = 500;
  int numEntries = 10;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    double num = 0.45 + (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "insert into foo values(" << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  txn_manager.CommitTransaction(txn);
  // Fetch values from the table
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string testQuery = "select increment(income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(testQuery.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (i)));
    double income = std::stod(result_income);
    EXPECT_DOUBLE_EQ(income, (inputs[i] + 1));
  }
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UDFTest, ComplexExpressionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE OR REPLACE FUNCTION complex_expr"
      "(a double, b double, c double, d double, e double)"
      " RETURNS double AS $$ BEGIN RETURN a * b - c + ( d * e);"
      " END; $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(income double);");
  // Adding in 2500 random decimal inputs between [-500, 500]
  int i;
  std::vector<double> inputs;
  int lo = 1;
  int hi = 500;
  int numEntries = 10;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    double num = 0.45 + (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "insert into foo values(" << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  txn_manager.CommitTransaction(txn);
  // Fetch values from the table
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string testQuery =
      "select complex_expr(income, income, income, income, income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(testQuery.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (i)));
    double income = std::stod(result_income);
    double res = (inputs[i] * inputs[i] - inputs[i]) + (inputs[i] * inputs[i]);
    EXPECT_EQ(round(income), round(res));
  }
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UDFTest, IfElseExpressionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE OR REPLACE FUNCTION if_else(a double)"
      " RETURNS double AS $$ BEGIN IF a < 1000 THEN"
      " RETURN a; ELSE RETURN a * 100; END IF; END; $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(income double);");
  // Adding in 2500 random decimal inputs between [-500, 500]
  int i;
  std::vector<double> inputs;
  int lo = -500;
  int hi = 500;
  int numEntries = 10;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    double num = 0.45 + (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "insert into foo values(" << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  txn_manager.CommitTransaction(txn);
  // Fetch values from the table
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string testQuery = "select if_else(income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(testQuery.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (i)));
    double income = std::stod(result_income);
    double res;
    if (inputs[i] < 1000)
      res = inputs[i];
    else
      res = inputs[i] * 100;
    EXPECT_DOUBLE_EQ(income, res);
  }
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(UDFTest, RecursiveFunctionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE OR REPLACE FUNCTION fib(i double)"
      " RETURNS double AS $$ BEGIN IF i < 3 THEN"
      " RETURN 1; ELSE RETURN fib(i-1) + fib(i-2);"
      " END IF; END; $$ LANGUAGE plpgsql;");

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(income double);");

  TestingSQLUtil::ExecuteSQLQuery("INSERT into foo values(10.0);");

  TestingSQLUtil::ExecuteSQLQuery("INSERT into foo values(20.0);");

  txn_manager.CommitTransaction(txn);
  // Fetch values from the table
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string testQuery = "select fib(income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(testQuery.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  std::vector<double> outputs;
  outputs.push_back(55.0);
  outputs.push_back(6765.0);
  int i;

  for (i = 0; i < 2; i++) {
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (i)));
    double income = std::stod(result_income);
    EXPECT_DOUBLE_EQ(income, (outputs[i]));
  }
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
