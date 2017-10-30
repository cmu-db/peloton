//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_groupby_sql_test.cpp
//
// Identification: test/sql/aggregate_groupby_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/optimizer.h"
#include "sql/testing_sql_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

class AggregateGroupBySQLTests : public PelotonTest {
 protected:
  void CreateAndLoadTable () {
    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d INT);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 2, 3, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 2, 3, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 2, 6, 1);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 2, 3, 2);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (5, 3, 6, 2);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (6, 4, 6, 2);");
  }

  void ExecuteSQLQueryAndCheckUnorderedResult (std::string query, std::unordered_set<std::string> ref_result) {
    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;

    // Execute query
    TestingSQLUtil::ExecuteSQLQuery(std::move(query), result, tuple_descriptor, rows_changed, error_message);
    unsigned int rows = result.size() / tuple_descriptor.size();


    // Build actual result as set of rows for comparison
    std::unordered_set<std::string> actual_result(rows);
    for (unsigned int i = 0; i < rows; i++) {
      std::string row_string;

      for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        row_string += TestingSQLUtil::GetResultValueAsString(result, i * tuple_descriptor.size() + j);
        if (j < tuple_descriptor.size() - 1) {
          row_string += "|";
        }
      }

      actual_result.insert(std::move(row_string));
    }


    // Compare actual result to expected result
    EXPECT_EQ(ref_result.size(), actual_result.size());

    for (auto &result_row : ref_result) {
      if (actual_result.count(result_row) == 0) {

        EXPECT_EQ(ref_result, actual_result);
        break;
      }
    }
  }
};

TEST_F (AggregateGroupBySQLTests, AggregateGroupByManyAVGsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT AVG(a), AVG(b), AVG(c), AVG(c), AVG(c) FROM test GROUP BY d;",
                                         {"5|3|5|5|5",
                                          "2|2|4|4|4"});


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F (AggregateGroupBySQLTests, AggregateGroupByMixedAVGsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // This test especially tests several AVGs in a row
  ExecuteSQLQueryAndCheckUnorderedResult("SELECT SUM(a), AVG(a), COUNT(b), AVG(b), MAX(c), AVG(c) FROM test GROUP BY d;",
                                         {"15|5|3|3|6|5",
                                          "6|2|3|2|6|4"});


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F (AggregateGroupBySQLTests, AggregateGroupByAllAggregationsSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ExecuteSQLQueryAndCheckUnorderedResult("SELECT AVG(a), SUM(a), MAX(a), MIN(a), COUNT(a) FROM test GROUP BY d;",
                                         {"2|6|3|1|3",
                                          "5|15|6|4|3"});


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F (AggregateGroupBySQLTests, AggregateGroupBySingleRowPerGroupSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // This test especially tests several AVGs in a row
  ExecuteSQLQueryAndCheckUnorderedResult("SELECT COUNT(*), MIN(b), MAX(c), AVG(d) FROM test GROUP BY a;",
                                         {"1|4|6|2",
                                          "1|2|3|1",
                                          "1|3|6|2",
                                          "1|2|3|2",
                                          "1|2|3|1",
                                          "1|2|6|1"});


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
