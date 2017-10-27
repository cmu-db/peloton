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

class DistinctAggregateSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(s SMALLINT, i INTEGER, bi BIGINT, r REAL, t TEXT, bp BPCHAR, vc VARCHAR, ts TIMESTAMP);");

  // Insert tuples into table
  for (int i = 0; i < 3; i++) {
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (1, 1, 1, 1.0, 'text1', '1', 'varchar1', '2016-06-22 19:10:21-00');");
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (2, 2, 2, 2.0, 'text2', '2', 'varchar2', '2016-06-22 19:10:22-00');");
    TestingSQLUtil::ExecuteSQLQuery(
        "INSERT INTO test VALUES (3, 3, 3, 3.0, 'text3', '3', 'varchar3', '2016-06-22 19:10:23-00');");
  }
}


TEST_F(DistinctAggregateSQLTests, DistinctAggregateCountTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), COUNT(distinct s), COUNT(distinct i), COUNT(distinct bi), COUNT(distinct r), COUNT(distinct t), COUNT(distinct bp), COUNT(distinct vc), COUNT(distinct ts) FROM test;", result,
  tuple_descriptor, rows_changed,
  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(1, result.size() / tuple_descriptor.size());
  EXPECT_EQ("9", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 8));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), MAX(distinct s), MAX(distinct i), MAX(distinct bi), MAX(distinct r), MAX(distinct t), MAX(distinct bp), MAX(distinct vc), MAX(distinct ts) FROM test;", result,
  tuple_descriptor, rows_changed,
  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(1, result.size() / tuple_descriptor.size());
  EXPECT_EQ("9", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("text3", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("varchar3", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("2016-06-22 19:10:23.000000+00", TestingSQLUtil::GetResultValueAsString(result, 8));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), MIN(distinct s), MIN(distinct i), MIN(distinct bi), MIN(distinct r), MIN(distinct t), MIN(distinct bp), MIN(distinct vc), MIN(distinct ts) FROM test;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(1, result.size() / tuple_descriptor.size());
  EXPECT_EQ("9", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("text1", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("varchar1", TestingSQLUtil::GetResultValueAsString(result, 7));
  EXPECT_EQ("2016-06-22 19:10:21.000000+00", TestingSQLUtil::GetResultValueAsString(result, 8));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), AVG(distinct s), AVG(distinct i), AVG(distinct bi), AVG(distinct r) FROM test;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(1, result.size() / tuple_descriptor.size());
  EXPECT_EQ("9", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 4));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), SUM(distinct s), SUM(distinct i), SUM(distinct bi), SUM(distinct r) FROM test;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(1, result.size() / tuple_descriptor.size());
  EXPECT_EQ("9", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("6", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("6", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("6", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("6", TestingSQLUtil::GetResultValueAsString(result, 4));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), COUNT(distinct i), COUNT(distinct bi), COUNT(distinct r), COUNT(distinct t), COUNT(distinct bp), COUNT(distinct vc), COUNT(distinct ts) FROM test GROUP BY s;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(3, result.size() / tuple_descriptor.size());

  int i = 0;
  // first row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  // second row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  // third row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), MAX(distinct i), MAX(distinct bi), MAX(distinct r), MAX(distinct t), MAX(distinct bp), MAX(distinct vc), MAX(distinct ts) FROM test GROUP BY s;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(3, result.size() / tuple_descriptor.size());
  
  int i = 0;
  // first row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:23.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));
  // second row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:22.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));
  // third row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:21.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), MIN(distinct i), MIN(distinct bi), MIN(distinct r), MIN(distinct t), MIN(distinct bp), MIN(distinct vc), MIN(distinct ts) FROM test GROUP BY s;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(3, result.size() / tuple_descriptor.size());

  int i = 0;
  // first row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:23.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));
  // second row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:22.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));
  // third row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("text1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("varchar1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2016-06-22 19:10:21.000000+00", TestingSQLUtil::GetResultValueAsString(result, i++));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), AVG(distinct i), AVG(distinct bi), AVG(distinct r) FROM test GROUP BY s;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(3, result.size() / tuple_descriptor.size());

  int i = 0;
  // first row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  // second row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  // third row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));

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


  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*), SUM(distinct i), SUM(distinct bi), SUM(distinct r) FROM test GROUP BY s;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ(3, result.size() / tuple_descriptor.size());

  int i = 0;
  // first row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  // second row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, i++));
  // third row
  EXPECT_EQ("3", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, i++));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}


}  // namespace test
}  // namespace peloton
