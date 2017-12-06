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

TEST_F(DecimalFunctionsSQLTest, FloorTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
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
    std::ostringstream os;
    os << "insert into foo values(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  txn_manager.CommitTransaction(txn);
  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::string testQuery = "select id, floor(income) from foo;";

  TestingSQLUtil::ExecuteSQLQuery(testQuery.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_income(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double income = std::stod(result_income);
    EXPECT_EQ(id, i);
    EXPECT_DOUBLE_EQ(income, floor(inputs[i]));
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// perform square root on the type decimal
TEST_F(DecimalFunctionsSQLTest, DecimalSqrtTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, number decimal);");
  // Adding in 500 random decimal inputs between [0, 500000]
  int i;
  std::vector<double> inputs;
  int lo = 0;
  int hi = 500000;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    double num = 0.45 + (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "INSERT INTO foo VALUES(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;
  os << "SELECT id, sqrt(number) FROM foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_number(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double number = std::stod(result_number);
    EXPECT_EQ(id, i);
    EXPECT_NEAR(number, sqrt(inputs[i]), 0.01);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// perform square root on the type tinyint
TEST_F(DecimalFunctionsSQLTest, TinyIntSqrtTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, number tinyint);");
  // Adding in 500 random decimal inputs between [0, 128)
  int i;
  std::vector<int8_t> inputs;
  int lo = 0;
  int hi = 128;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    int32_t num = (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "INSERT INTO foo VALUES(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;
  os << "SELECT id, sqrt(number) FROM foo ORDER BY id;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_number(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double number = std::stod(result_number);
    EXPECT_EQ(id, i);
    EXPECT_NEAR(number, sqrt(inputs[i]), 0.01);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// perform square root on the type smallint
TEST_F(DecimalFunctionsSQLTest, SmallIntSqrtTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, number smallint);");
  // Adding in 500 random decimal inputs between [0, 32768)
  int i;
  std::vector<int16_t> inputs;
  int lo = 0;
  int hi = 32768;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    int32_t num = (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "INSERT INTO foo VALUES(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;
  os << "SELECT id, sqrt(number) FROM foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_number(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double number = std::stod(result_number);
    EXPECT_EQ(id, i);
    EXPECT_NEAR(number, sqrt(inputs[i]), 0.01);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// perform square root on the type int
TEST_F(DecimalFunctionsSQLTest, IntegerSqrtTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(id integer, number int);");
  // Adding in 500 random decimal inputs between [0, 2147483648)
  int i;
  std::vector<int32_t> inputs;
  int lo = 0;
  int hi = 2147483648;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    int32_t num = (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "INSERT INTO foo VALUES(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;
  os << "SELECT id, sqrt(number) FROM foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_number(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double number = std::stod(result_number);
    EXPECT_EQ(id, i);
    EXPECT_NEAR(number, sqrt(inputs[i]), 0.05);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// perform square root on the type big int
TEST_F(DecimalFunctionsSQLTest, BigIntSqrtTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a txn
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, number bigint);");
  // Adding in 500 random decimal inputs between [0, 32768)
  int i;
  std::vector<int64_t> inputs;
  int lo = 0;
  int hi = 2147483648;
  int numEntries = 500;
  // Setting a seed
  std::srand(std::time(0));
  for (i = 0; i < numEntries; i++) {
    int64_t num = (std::rand() % (hi - lo));
    inputs.push_back(num);
    std::ostringstream os;
    os << "INSERT INTO foo VALUES(" << i << ", " << num << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, numEntries);

  // Fetch values from the table
  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;
  os << "SELECT id, sqrt(number) FROM foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < numEntries; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_number(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    double number = std::stod(result_number);
    EXPECT_EQ(id, i);
    EXPECT_NEAR(number, sqrt(inputs[i]), 0.05);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
