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

class DecimalSQLTestsBase : public PelotonTest {
  protected:
    virtual void SetUp() override {
      // Call parent virtual function first
      PelotonTest::SetUp();
      CreateDB();
    }
    /*** Helper functions **/
    void CreateDB() {
      // Create database
      auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);
    }

  void CreateTableWithCol(std::string coltype) {
      // Create Table
      auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      std::ostringstream os;
      os << "CREATE TABLE foo(id integer, income " << coltype << ");";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
      txn_manager.CommitTransaction(txn);
    }

    virtual void TearDown() override {
      // Destroy test database
      auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);

      // Call parent virtual function
      PelotonTest::TearDown();
    }
};

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
    std::vector<ResultValue> result;
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

  TEST_F(DecimalSQLTestsBase, TinyIntAbsTest) {
    CreateTableWithCol("tinyint");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Adding in 200 random decimal inputs between [-127, 127]
    int i;
    std::vector<int8_t> inputs;
    int lo = 0;
    int hi = 254;
    int numEntries = 200;
    // Setting a seed
    std::srand(std::time(0));
    for (i = 0; i < numEntries; i++) {
      int32_t num = (std::rand() % (hi - lo)) - 127;
      inputs.push_back(num);
      std::ostringstream os;
      os << "insert into foo values(" << i << ", " << num << ");";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
    }
    EXPECT_EQ(i, numEntries);
    txn_manager.CommitTransaction(txn);

    // Fetch values from the table
    std::vector<ResultValue> result;
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
      int income = std::stoi(result_income);
      EXPECT_EQ(id, i);
      EXPECT_EQ(income, abs(inputs[i]));
    }
  }

  TEST_F(DecimalSQLTestsBase, SmallIntAbsTest) {
    CreateTableWithCol("smallint");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Adding in 200 random integer inputs between [-32767, 32767]
    int i;
    std::vector<int16_t> inputs;
    int lo = 0;
    int hi = 65534;
    int numEntries = 200;
    // Setting a seed
    std::srand(std::time(0));
    for (i = 0; i < numEntries; i++) {
      int32_t num = (std::rand() % (hi - lo)) - 32767;
      inputs.push_back(num);
      std::ostringstream os;
      os << "insert into foo values(" << i << ", " << num << ");";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
    }
    EXPECT_EQ(i, numEntries);
    txn_manager.CommitTransaction(txn);

    // Fetch values from the table
    std::vector<ResultValue> result;
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
      int income = std::stoi(result_income);
      EXPECT_EQ(id, i);
      EXPECT_EQ(income, abs(inputs[i]));
    }
  }

  TEST_F(DecimalSQLTestsBase, IntAbsTest) {
    CreateTableWithCol("int");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Adding in 200 random integer inputs between [-32767, 32767]
    int i;
    std::vector<int32_t> inputs;
    int lo = 0;
    int hi = 65534;
    int numEntries = 200;
    // Setting a seed
    std::srand(std::time(0));
    for (i = 0; i < numEntries; i++) {
      int32_t num = (std::rand() % (hi - lo)) - 32767;
      inputs.push_back(num);
      std::ostringstream os;
      os << "insert into foo values(" << i << ", " << num << ");";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
    }
    EXPECT_EQ(i, numEntries);
    txn_manager.CommitTransaction(txn);

    // Fetch values from the table
    std::vector<ResultValue> result;
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
      int income = std::stoi(result_income);
      EXPECT_EQ(id, i);
      EXPECT_EQ(income, abs(inputs[i]));
    }
  }

  TEST_F(DecimalSQLTestsBase, BigIntAbsTest) {
    CreateTableWithCol("bigint");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Adding in 200 random integer inputs between [-32767, 32767]
    int i;
    std::vector<int32_t> inputs;
    int lo = 0;
    int hi = 65534;
    int numEntries = 200;
    // Setting a seed
    std::srand(std::time(0));
    for (i = 0; i < numEntries; i++) {
      int32_t num = (std::rand() % (hi - lo)) - 32767;
      inputs.push_back(num);
      std::ostringstream os;
      os << "insert into foo values(" << i << ", " << num << ");";
      TestingSQLUtil::ExecuteSQLQuery(os.str());
    }
    EXPECT_EQ(i, numEntries);
    txn_manager.CommitTransaction(txn);

    // Fetch values from the table
    std::vector<ResultValue> result;
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
      int income = std::stoi(result_income);
      EXPECT_EQ(id, i);
      EXPECT_EQ(income, abs(inputs[i]));
    }
  }

  TEST_F(DecimalSQLTestsBase, DecimalAbsTest) {
    CreateTableWithCol("decimal");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
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
    std::vector<ResultValue> result;
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
  }

  TEST_F(DecimalFunctionsSQLTest, CeilTest) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    catalog::Catalog::GetInstance()->Bootstrap();
    txn_manager.CommitTransaction(txn);
    // Create a t
    txn = txn_manager.BeginTransaction();

    TestingSQLUtil::ExecuteSQLQuery(
            "CREATE TABLE foo(id integer, income decimal);");

    // Adding in 500 random decimal inputs between [-500, 500]
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
    std::vector<ResultValue> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected;
    std::string testQuery = "select id, ceil(income) from foo;";

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
      EXPECT_DOUBLE_EQ(income, ceil(inputs[i]));
    }

    testQuery = "select id, ceiling(income) from foo;";

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
      EXPECT_DOUBLE_EQ(income, ceil(inputs[i]));
    }

    // free the database just created
    txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

}  // namespace test
}  // namespace peloton
