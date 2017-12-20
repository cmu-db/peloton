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
