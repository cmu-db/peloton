//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity_test.cpp
//
// Identification: test/optimizer/selectivity_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <vector>

#include "common/logger.h"
#include "optimizer/stats/selectivity.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "executor/testing_executor_util.h"

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class SelectivityTests : public PelotonTest {};

const std::string TEST_TABLE_NAME = "test";

const double DEFAULT_SELECTIVITY_OFFSET = 0.01;

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(id INT PRIMARY KEY, b DECIMAL, c VARCHAR);");

  // Insert tuples into table
  // TestingSQLUtil::ExecuteSQLQuery(
  //     "INSERT INTO test VALUES (1, 1.1, 'one');");
  // TestingSQLUtil::ExecuteSQLQuery(
  //     "INSERT INTO test VALUES (2, 2.2, 'two');");
  // TestingSQLUtil::ExecuteSQLQuery(
  //     "INSERT INTO test VALUES (3, 3.3, 'three');");
}

void ExpectSelectivityEqual(double actual, double expected,
                            double offset = DEFAULT_SELECTIVITY_OFFSET) {
  EXPECT_GE(actual, expected - offset);
  EXPECT_LE(actual, expected + offset);
}

TEST_F(SelectivityTests, RangeSelectivityTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // Create uniform table stats
  int nrow = 100000;
  for (int i = 1; i <= nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i << ", 1.1, 'abcd');";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  auto catalog = catalog::Catalog::GetInstance();
  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME);
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, TEST_TABLE_NAME);
  oid_t db_id = database->GetOid();
  oid_t table_id = table->GetOid();

  type::Value value1 = type::ValueFactory::GetIntegerValue(nrow / 4);

  // Check for default selectivity when table stats does not exist.
  double sel = Selectivity::GetLessThanSelectivity(db_id, table_id, 0, value1);
  EXPECT_EQ(sel, DEFAULT_SELECTIVITY);

  // Run analyze
  TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");

  // Check new selectivity
  double less_than_sel = Selectivity::GetLessThanSelectivity(db_id, table_id, 0, value1);
  ExpectSelectivityEqual(less_than_sel, 0.25);
  // double greater_than_sel = Selectivity::GetGreaterThanSelectivity(db_id, table_id, 0, value1);
  // ExpectSelectivityEqual(greater_than_sel, 0.75);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(SelectivityTests, LikeSelectivityTest) {
  const int tuple_count = 1000;
  const int tuple_per_tilegroup = 100;

  // Create a table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  // Collect samples and add samples.
  txn = txn_manager.BeginTransaction();
  TupleSamplesStorage *tuple_samples_storage =
      TupleSamplesStorage::GetInstance();
  tuple_samples_storage->CollectSamplesForTable(data_table.get(), txn);
  txn_manager.CommitTransaction(txn);

  oid_t db_id = data_table->GetDatabaseOid();
  oid_t table_id = data_table->GetOid();

  double like_than_sel_0 = Selectivity::GetLikeSelectivity(db_id, table_id, 0, "a");
  double like_than_sel_1 = Selectivity::GetLikeSelectivity(db_id, table_id, 1, "a");
  double like_than_sel_2 = Selectivity::GetLikeSelectivity(db_id, table_id, 2, "a");
  double like_than_sel_3 = Selectivity::GetLikeSelectivity(db_id, table_id, 3, "a");
  (void) like_than_sel_3;

  EXPECT_EQ(like_than_sel_0, 0);
  EXPECT_EQ(like_than_sel_1, 0);
  EXPECT_EQ(like_than_sel_2, 0);

}
} /* namespace test */
} /* namespace peloton */
