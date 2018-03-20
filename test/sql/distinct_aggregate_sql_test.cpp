//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// distinct_aggregate_sql_test.cpp
//
// Identification: test/sql/distinct_aggregate_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class DistinctAggregateSQLTests : public PelotonTest {
 protected:
  void CreateAndLoadTable() {
    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(s SMALLINT, i INTEGER, bi BIGINT, r REAL, t TEXT, "
        "bp BPCHAR, vc VARCHAR, ts TIMESTAMP);");

    // Insert tuples into table
    for (int i = 0; i < 3; i++) {
      TestingSQLUtil::ExecuteSQLQuery(
          "INSERT INTO test VALUES (1, 1, 1, 1.0, 'text1', '1', 'varchar1', "
          "'2016-06-22 19:10:21-00');");
      TestingSQLUtil::ExecuteSQLQuery(
          "INSERT INTO test VALUES (2, 2, 2, 2.0, 'text2', '2', 'varchar2', "
          "'2016-06-22 19:10:22-00');");
      TestingSQLUtil::ExecuteSQLQuery(
          "INSERT INTO test VALUES (3, 3, 3, 3.0, 'text3', '3', 'varchar3', "
          "'2016-06-22 19:10:23-00');");
    }
  }

  void ExecuteSQLQueryAndCheckUnorderedResult(
      std::string query, std::unordered_set<std::string> ref_result) {
    std::vector<ResultValue> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;

    // Execute query
    TestingSQLUtil::ExecuteSQLQuery(std::move(query), result, tuple_descriptor,
                                    rows_changed, error_message);
    unsigned int rows = result.size() / tuple_descriptor.size();

    // Build actual result as set of rows for comparison
    std::unordered_set<std::string> actual_result(rows);
    for (unsigned int i = 0; i < rows; i++) {
      std::string row_string;

      for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        row_string += TestingSQLUtil::GetResultValueAsString(
            result, i * tuple_descriptor.size() + j);
        if (j < tuple_descriptor.size() - 1) {
          row_string += "|";
        }
      }

      actual_result.insert(std::move(row_string));
    }

    // Compare actual result to expected result
    EXPECT_EQ(ref_result.size(), actual_result.size());

    for (auto &result_row : ref_result) {
      if (actual_result.erase(result_row) == 0) {
        EXPECT_EQ(ref_result, actual_result);
        break;
      }
    }
  }
};

TEST_F(DistinctAggregateSQLTests, DistinctAggregateCountTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), COUNT(distinct s), COUNT(distinct i), COUNT(distinct "
      "bi), COUNT(distinct r), COUNT(distinct t), COUNT(distinct bp), "
      "COUNT(distinct vc), COUNT(distinct ts) FROM test;",
      {"9|3|3|3|3|3|3|3|3"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateMaxTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), MAX(distinct s), MAX(distinct i), MAX(distinct bi), "
      "MAX(distinct r), MAX(distinct t), MAX(distinct bp), MAX(distinct vc), "
      "MAX(distinct ts) FROM test;",
      {"9|3|3|3|3|text3|3|varchar3|2016-06-22 19:10:23.000000+00"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateMinTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), MIN(distinct s), MIN(distinct i), MIN(distinct bi), "
      "MIN(distinct r), MIN(distinct t), MIN(distinct bp), MIN(distinct vc), "
      "MIN(distinct ts) FROM test;",
      {"9|1|1|1|1|text1|1|varchar1|2016-06-22 19:10:21.000000+00"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateAvgTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), AVG(distinct s), AVG(distinct i), AVG(distinct bi), "
      "AVG(distinct r) FROM test;",
      {"9|2|2|2|2"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateSumTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), SUM(distinct s), SUM(distinct i), SUM(distinct bi), "
      "SUM(distinct r) FROM test;",
      {"9|6|6|6|6"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateGroupByCountTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), COUNT(distinct i), COUNT(distinct bi), COUNT(distinct "
      "r), COUNT(distinct t), COUNT(distinct bp), COUNT(distinct vc), "
      "COUNT(distinct ts) FROM test GROUP BY s;",
      {"3|1|1|1|1|1|1|1", "3|1|1|1|1|1|1|1", "3|1|1|1|1|1|1|1"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateGroupByMaxTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), MAX(distinct i), MAX(distinct bi), MAX(distinct r), "
      "MAX(distinct t), MAX(distinct bp), MAX(distinct vc), MAX(distinct ts) "
      "FROM test GROUP BY s;",
      {"3|3|3|3|text3|3|varchar3|2016-06-22 19:10:23.000000+00",
       "3|2|2|2|text2|2|varchar2|2016-06-22 19:10:22.000000+00",
       "3|1|1|1|text1|1|varchar1|2016-06-22 19:10:21.000000+00"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateGroupByMinTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), MIN(distinct i), MIN(distinct bi), MIN(distinct r), "
      "MIN(distinct t), MIN(distinct bp), MIN(distinct vc), MIN(distinct ts) "
      "FROM test GROUP BY s;",
      {"3|3|3|3|text3|3|varchar3|2016-06-22 19:10:23.000000+00",
       "3|2|2|2|text2|2|varchar2|2016-06-22 19:10:22.000000+00",
       "3|1|1|1|text1|1|varchar1|2016-06-22 19:10:21.000000+00"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateGroupByAvgTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), AVG(distinct i), AVG(distinct bi), AVG(distinct r) "
      "FROM test GROUP BY s;",
      {"3|3|3|3", "3|2|2|2", "3|1|1|1"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DistinctAggregateSQLTests, DistinctAggregateGroupBySumTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult(
      "SELECT COUNT(*), SUM(distinct i), SUM(distinct bi), SUM(distinct r) "
      "FROM test GROUP BY s;",
      {"3|3|3|3", "3|2|2|2", "3|1|1|1"});

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
