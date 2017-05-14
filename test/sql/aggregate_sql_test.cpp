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

class AggregateSQLTests : public PelotonTest {};

TEST_F(AggregateSQLTests, EmptyTableTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE xxx(a INT PRIMARY KEY, b INT);");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  // All of these aggregates should return null
  std::vector<std::string> nullAggregates = {"MIN", "MAX", "AVG", "SUM"};
  std::vector<type::Type::TypeId> expectedTypes = {
      type::Type::INTEGER, type::Type::INTEGER, type::Type::DECIMAL,
      type::Type::INTEGER};
  for (int i = 0; i < (int)nullAggregates.size(); i++) {
    std::string expected;

    std::ostringstream os;
    os << "SELECT " << nullAggregates[i] << "(b) FROM xxx";
    TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                    rows_affected, error_message);
    std::string resultStr(result[0].second.begin(), result[0].second.end());

    EXPECT_EQ(expected, resultStr);
  }

  // These aggregates should return zero
  std::vector<std::string> zeroAggregates = {"COUNT"};
  for (int i = 0; i < (int)zeroAggregates.size(); i++) {
    std::string expected = type::ValueFactory::GetIntegerValue(0).ToString();

    std::ostringstream os;
    os << "SELECT " << zeroAggregates[i] << "(b) FROM xxx";
    TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                    rows_affected, error_message);
    std::string resultStr(result[0].second.begin(), result[0].second.end());

    EXPECT_EQ(expected, resultStr);
  }
}

TEST_F(AggregateSQLTests, MinMaxTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Create a table first
  // TODO: LM: I didn't test boolean here because we can't insert booleans
  // into the table
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b SMALLINT, c "
      "INT, d BIGINT, e DECIMAL, f DOUBLE, g VARCHAR, h TIMESTAMP);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 2, 2, 2, 2.0, "
      "2.0, '23', '2016-12-06 00:00:02-04');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (5, 2, 2, 2, 2.0, "
      "2.0, null, '2016-12-06 00:00:02-04');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 1, 1, 1, 1.0, "
      "1.0, '15', '2016-12-06 00:00:01-04');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (4, 4, 4, 4, 4.0, "
      "4.0, '41', '2016-12-06 00:00:04-04');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 3, 3, 3, 3.0, "
      "3.0, '33', '2016-12-06 00:00:03-04');");

  TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  // test small int
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(b) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(result[0].second[0], '1');
  // Check the return type
  // TODO: LM: Right now we cannot deduce TINYINT
  // EXPECT_EQ(PostgresValueType::TINYINT, std::get<1>(tuple_descriptor[0]));

  TestingSQLUtil::ExecuteSQLQuery("SELECT max(b) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '4');
  // EXPECT_EQ(PostgresValueType::TINYINT, std::get<1>(tuple_descriptor[0]));

  // test int
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(a) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(result[0].second[0], '1');
  // Check the return type
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::INTEGER),
            std::get<1>(tuple_descriptor[0]));

  TestingSQLUtil::ExecuteSQLQuery("SELECT max(a) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '4');
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::INTEGER),
            std::get<1>(tuple_descriptor[0]));

  // test big int
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(d) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '1');
  // Check the return type
  // TODO: LM: Right now we cannot deduce BIGINT
  // EXPECT_EQ(PostgresValueType::BIGINT,
  //          std::get<1>(tuple_descriptor[0]));

  TestingSQLUtil::ExecuteSQLQuery("SELECT max(d) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '4');
  // EXPECT_EQ(PostgresValueType::BIGINT,
  //          std::get<1>(tuple_descriptor[0]));

  // test double
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(e) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '1');
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::DOUBLE),
            std::get<1>(tuple_descriptor[0]));

  TestingSQLUtil::ExecuteSQLQuery("SELECT max(e) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '4');
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::DOUBLE),
            std::get<1>(tuple_descriptor[0]));

  // test decimal
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(f) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '1');
  // Right now we treat all double and decimal as double
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::DOUBLE),
            std::get<1>(tuple_descriptor[0]));

  TestingSQLUtil::ExecuteSQLQuery("SELECT max(f) from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '4');
  EXPECT_EQ(static_cast<oid_t>(PostgresValueType::DOUBLE),
            std::get<1>(tuple_descriptor[0]));

  // test varchar
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(g) from test", result);
  EXPECT_EQ(result[0].second[0], '1');
  TestingSQLUtil::ExecuteSQLQuery("SELECT max(g) from test", result);
  EXPECT_EQ(result[0].second[0], '4');

  // test timestamp
  TestingSQLUtil::ExecuteSQLQuery("SELECT min(h) from test", result);
  EXPECT_EQ(result[0].second[18], '1');
  TestingSQLUtil::ExecuteSQLQuery("SELECT max(h) from test", result);
  EXPECT_EQ(result[0].second[18], '4');

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

    TEST_F(AggregateSQLTests, SumTest) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

      // Create a table first
      // TODO: LM: I didn't test boolean here because we can't insert booleans
      // into the table
      TestingSQLUtil::ExecuteSQLQuery(
              "CREATE TABLE test(a INT PRIMARY KEY, b SMALLINT, c "
                      "INT, d BIGINT, e DECIMAL, f DOUBLE, g VARCHAR, h TIMESTAMP);");

      // Insert tuples into table
      TestingSQLUtil::ExecuteSQLQuery(
              "INSERT INTO test VALUES (2, 2, 2, 2, 2.0, "
                      "2.0, '23', '2016-12-06 00:00:02-04');");
      TestingSQLUtil::ExecuteSQLQuery(
              "INSERT INTO test VALUES (5, 2, 2, 2, 2.0, "
                      "2.0, null, '2016-12-06 00:00:02-04');");
      TestingSQLUtil::ExecuteSQLQuery(
              "INSERT INTO test VALUES (1, 1, 1, 1, 1.0, "
                      "1.0, '15', '2016-12-06 00:00:01-04');");
      TestingSQLUtil::ExecuteSQLQuery(
              "INSERT INTO test VALUES (4, 4, 4, 4, 4.0, "
                      "4.0, '41', '2016-12-06 00:00:04-04');");
      TestingSQLUtil::ExecuteSQLQuery(
              "INSERT INTO test VALUES (3, 3, 3, 3, 3.0, "
                      "3.0, '33', '2016-12-06 00:00:03-04');");

      TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

      std::vector<StatementResult> result;
      std::vector<FieldInfo> tuple_descriptor;
      std::string error_message;
      int rows_affected;
      optimizer::SimpleOptimizer optimizer;

      // test small int
      TestingSQLUtil::ExecuteSQLQuery("SELECT sum(b) from test", result,
                                      tuple_descriptor, rows_affected,
                                      error_message);
      // Check the return value
      EXPECT_EQ(result[0].second[0], '1');

      // test int
      TestingSQLUtil::ExecuteSQLQuery("SELECT sum(a) from test", result,
                                      tuple_descriptor, rows_affected,
                                      error_message);
      // Check the return value
      EXPECT_EQ(result[0].second[0], '1');

      // test big int
      TestingSQLUtil::ExecuteSQLQuery("SELECT sum(d) from test", result,
                                      tuple_descriptor, rows_affected,
                                      error_message);
      EXPECT_EQ(result[0].second[1], '2');

      // test double
      TestingSQLUtil::ExecuteSQLQuery("SELECT sum(e) from test", result,
                                      tuple_descriptor, rows_affected,
                                      error_message);
      EXPECT_EQ(result[0].second[1], '2');

      // test decimal
      TestingSQLUtil::ExecuteSQLQuery("SELECT sum(f) from test", result,
                                      tuple_descriptor, rows_affected,
                                      error_message);
      EXPECT_EQ(result[0].second[1], '2');

      // free the database just created
      catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);
    }

    TEST_F(AggregateSQLTests, AvgTest) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

      // Create a table first
      // TODO: LM: I didn't test boolean here because we can't insert booleans
      // into the table
      TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b SMALLINT, c "
      "INT, d BIGINT, e DECIMAL, f DOUBLE, g VARCHAR, h TIMESTAMP);");

      // Insert tuples into table
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 2, 2, 2, 2.0, "
      "2.0, '23', '2016-12-06 00:00:02-04');");
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (5, 2, 2, 2, 2.0, "
      "2.0, null, '2016-12-06 00:00:02-04');");
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 1, 1, 1, 1.0, "
      "1.0, '15', '2016-12-06 00:00:01-04');");
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (4, 4, 4, 4, 4.0, "
      "4.0, '41', '2016-12-06 00:00:04-04');");
      TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 3, 3, 3, 3.0, "
      "3.0, '33', '2016-12-06 00:00:03-04');");

      TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

      std::vector<StatementResult> result;
      std::vector<FieldInfo> tuple_descriptor;
      std::string error_message;
      int rows_affected;
      optimizer::SimpleOptimizer optimizer;

      // test small int
      TestingSQLUtil::ExecuteSQLQuery("SELECT avg(b) from test", result,
      tuple_descriptor, rows_affected,
      error_message);
      // Check the return value
      EXPECT_EQ(result[0].second[0], '2');

      // test int
      TestingSQLUtil::ExecuteSQLQuery("SELECT avg(a) from test", result,
      tuple_descriptor, rows_affected,
      error_message);
      // Check the return value
      EXPECT_EQ(result[0].second[0], '3');

      // test big int
      TestingSQLUtil::ExecuteSQLQuery("SELECT avg(d) from test", result,
      tuple_descriptor, rows_affected,
      error_message);
      EXPECT_EQ(result[0].second[0], '2');

      // test double
      TestingSQLUtil::ExecuteSQLQuery("SELECT avg(e) from test", result,
      tuple_descriptor, rows_affected,
      error_message);
      EXPECT_EQ(result[0].second[0], '2');

      // test decimal
      TestingSQLUtil::ExecuteSQLQuery("SELECT avg(f) from test", result,
      tuple_descriptor, rows_affected,
      error_message);
      EXPECT_EQ(result[0].second[0], '2');

      // free the database just created
      catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);
    }

    TEST_F(AggregateSQLTests, CountTest) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

    // Create a table first
    // TODO: LM: I didn't test boolean here because we can't insert booleans
    // into the table
    TestingSQLUtil::ExecuteSQLQuery(
    "CREATE TABLE test(a INT PRIMARY KEY, b SMALLINT, c "
    "INT, d BIGINT, e DECIMAL, f DOUBLE, g VARCHAR, h TIMESTAMP);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (2, 2, 2, 2, 2.0, "
    "2.0, '23', '2016-12-06 00:00:02-04');");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (5, 2, 2, 2, 2.0, "
    "2.0, null, '2016-12-06 00:00:02-04');");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (1, 1, 1, 1, 1.0, "
    "1.0, '15', '2016-12-06 00:00:01-04');");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (4, 4, 4, 4, 4.0, "
    "4.0, '41', '2016-12-06 00:00:04-04');");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (3, 3, 3, 3, 3.0, "
    "3.0, '33', '2016-12-06 00:00:03-04');");

    TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected;
    optimizer::SimpleOptimizer optimizer;

    // test small int
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '5');

    // test int
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(a) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '5');

    // test big int
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(d) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '5');

    // test double
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(e) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '5');

    // test decimal
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(f) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '5');

    // test *
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(*) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '5');

    // free the database just created
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
    }

    TEST_F(AggregateSQLTests, ExpressionTest) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

    // Create a table first
    // TODO: LM: I didn't test boolean here because we can't insert booleans
    // into the table
    TestingSQLUtil::ExecuteSQLQuery(
    "CREATE TABLE test(a INT PRIMARY KEY, b INT);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (3, 4);");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (1, 2);");
    TestingSQLUtil::ExecuteSQLQuery(
    "INSERT INTO test VALUES (1, 2);");

    TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected;
    optimizer::SimpleOptimizer optimizer;

    // test COUNT + arithmetic: ADD
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(a+b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '1');

    // test AVG + arithmetic: ADD
    TestingSQLUtil::ExecuteSQLQuery("SELECT avg(a+b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '4');

    // test MIN + arithmetic: ADD
    TestingSQLUtil::ExecuteSQLQuery("SELECT min(a+b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '3');

    // test MAX + arithemtic: MAX
    TestingSQLUtil::ExecuteSQLQuery("SELECT max(a+b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '7');

    // test COUNT + arithmetic: MUL
    TestingSQLUtil::ExecuteSQLQuery("SELECT count(a*b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '3');

    // test AVG + arithmetic: MUL
    TestingSQLUtil::ExecuteSQLQuery("SELECT avg(a*b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    // Check the return value
    EXPECT_EQ(result[0].second[0], '5');

    // test MIN + arithmetic: MUL
    TestingSQLUtil::ExecuteSQLQuery("SELECT min(a*b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '2');

    // test MAX + arithemtic: MUL
    TestingSQLUtil::ExecuteSQLQuery("SELECT max(a*b) from test", result,
    tuple_descriptor, rows_affected,
    error_message);
    EXPECT_EQ(result[0].second[0], '1');
    // free the database just created
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
    }

}  // namespace test
}  // namespace peloton
