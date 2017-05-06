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
#include "optimizer/stats/sel_param.h"

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class SelectivityTests : public PelotonTest {};

const std::string TEST_TABLE_NAME = "test";

// Equality checking accuracy offset
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

// Utility function for compare approximate equality.
void ExpectSelectivityEqual(double actual, double expected,
                            double offset = DEFAULT_SELECTIVITY_OFFSET) {
  EXPECT_GE(actual, expected - offset);
  EXPECT_LE(actual, expected + offset);
}

// Test range selectivity including >, <, >= and <= using uniform dataset
TEST_F(SelectivityTests, RangeSelectivityTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // Create **uniform** table stats
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
  oid_t column_id = 0; // first column
  type::Value value1 = type::ValueFactory::GetIntegerValue(nrow / 4);
  SelParam param{db_id, table_id, column_id, value1};

  // Check for default selectivity when table stats does not exist.
  double default_sel = Selectivity::LessThan(&param);
  EXPECT_EQ(default_sel, DEFAULT_SELECTIVITY);

  // Run analyze
  TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");

  // Check new selectivity
  double less_than_sel = Selectivity::LessThan(&param);
  ExpectSelectivityEqual(less_than_sel, 0.25);
  // double greater_than_sel = Selectivity::GreaterThanSel(&params);
  // ExpectSelectivityEqual(greater_than_sel, 0.75);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// Test LIKE operator selectivity
// Note LIKE operator is not yet implemented so this test only
// checks if getting column stats and sampling works.
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
  TupleSamplesStorage *tuple_samples_storage = TupleSamplesStorage::GetInstance();
  tuple_samples_storage->CollectSamplesForTable(data_table.get(), txn);
  txn_manager.CommitTransaction(txn);

  oid_t db_id = data_table->GetDatabaseOid();
  oid_t table_id = data_table->GetOid();
  type::Value value = type::ValueFactory::GetVarcharValue("a");

  SelParam param1{db_id, table_id, 0, value};
  SelParam param2{db_id, table_id, 1, value};
  SelParam param3{db_id, table_id, 2, value};
  SelParam param4{db_id, table_id, 3, value};

  double like_than_sel_1 = Selectivity::Like(&param1);
  double like_than_sel_2 = Selectivity::Like(&param2);
  double like_than_sel_3 = Selectivity::Like(&param3);
  double like_than_sel_4 = Selectivity::Like(&param4);

  EXPECT_EQ(like_than_sel_1, DEFAULT_SELECTIVITY);
  EXPECT_EQ(like_than_sel_2, DEFAULT_SELECTIVITY);
  EXPECT_EQ(like_than_sel_3, DEFAULT_SELECTIVITY);
  EXPECT_EQ(like_than_sel_4, DEFAULT_SELECTIVITY);
}


TEST_F(SelectivityTests, EqualSelectivityTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  int nrow = 1000;
  for (int i = 1; i <= nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i << ", " << i % 3 + 1 << ", 1.21);";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  auto catalog = catalog::Catalog::GetInstance();
  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME);
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, TEST_TABLE_NAME);
  oid_t db_id = database->GetOid();
  oid_t table_id = table->GetOid();
  oid_t column_id1 = 1;

  type::Value value1 = type::ValueFactory::GetDecimalValue(1.0);

  // Check for default selectivity when table stats does not exist.
  SelParam param1{db_id, table_id, column_id1, value1};
  double sel = Selectivity::Equal(&param1);
  EXPECT_EQ(sel, DEFAULT_SELECTIVITY);

  // Run analyze
  TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");

  // Check selectivity
  // equal, in mcv
  double eq_sel_in_mcv = Selectivity::Equal(&param1);
  double neq_sel_in_mcv = Selectivity::NotEqual(&param1);
  ExpectSelectivityEqual(eq_sel_in_mcv, 0.33);
  ExpectSelectivityEqual(neq_sel_in_mcv, 0.67);

  // Add other values into the table
  // default top_k == 10, so add another 10 - 3 = 7 elements (4-10)
  for (int i = 1; i <= nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i + 1000 << ", " << i % 7 + 4 << ", 1.21);";
    //os << "INSERT INTO test VALUES (" << i << ", 1.1, 1.21);";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  // these elements will not be in mcv
  for (int i = 1; i <= nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i + 2000 << ", " << i % 50 + 11 << ", 1.21);";
    //os << "INSERT INTO test VALUES (" << i << ", 1.1, 1.21);";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  // Run analyze
  TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");

  // Check selectivity
  // equal, not in mcv
  oid_t column_id2 = 0;
  type::Value value2 = type::ValueFactory::GetDecimalValue(20.0);
  SelParam param2{db_id, table_id, column_id2, value2};

  double eq_sel_nin_mcv = Selectivity::Equal(&param2);
  double neq_sel_nin_mcv = Selectivity::NotEqual(&param2);
  // (1 - 2/3) / (3 + 7 + 50 - 10) = 1 / 150 = 0.01667
  ExpectSelectivityEqual(eq_sel_nin_mcv, 0.0066, 0.01);
  ExpectSelectivityEqual(neq_sel_nin_mcv, 0.9933, 0.01);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */

