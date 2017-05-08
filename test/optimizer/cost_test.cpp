//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_test.cpp
//
// Identification: test/optimizer/cost_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#define private public

#include <vector>
#include <sstream>
#include <string>
#include <iostream>

#include "common/logger.h"
#include "optimizer/stats/cost.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/value_condition.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/cost.h"
#include "type/types.h"
#include "type/value_factory.h"
#include "executor/testing_executor_util.h"
#include "catalog/column_catalog.h"
#include "catalog/catalog.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

using namespace optimizer;

class CostTests : public PelotonTest {};

// tablename: test
// database name: DEFAULT_DB_NAME
void CreateAndLoadTable() {
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR, salary DECIMAL);");
  int nrow = 10000; // 10k
  for (int i = 1; i <= nrow; i++) {
    std::ostringstream os;
    os << "INSERT INTO test VALUES (" << i << ", 'name', 1.1);";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
}

std::shared_ptr<TableStats> GetTableStatsWithName(std::string table_name) {
  auto catalog = catalog::Catalog::GetInstance();
  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME);
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, table_name);
  oid_t db_id = database->GetOid();
  oid_t table_id = table->GetOid();
  auto stats_storage = StatsStorage::GetInstance();
  auto table_stats = stats_storage->GetTableStats(db_id, table_id);
  return table_stats;
}

TEST_F(CostTests, SimpleCostTest) {

  TableStats table_stats{};
  type::Value value = type::ValueFactory::GetIntegerValue(1);
  ValueCondition condition{0, ExpressionType::COMPARE_EQUAL, value};

  double cost = Cost::SingleConditionSeqScanCost(&table_stats, &condition);
  EXPECT_EQ(cost, 0);
}

TEST_F(CostTests, ScanCostTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // create table with name test
  CreateAndLoadTable();

  // Collect stats
  TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");

  auto table_stats = GetTableStatsWithName("test");
  EXPECT_NE(table_stats, nullptr);
  EXPECT_EQ(table_stats->num_rows, 10000);

  // condition1: id < 1000
  type::Value value1 = type::ValueFactory::GetIntegerValue(1000);
  ValueCondition condition1{0, "id", ExpressionType::COMPARE_LESSTHAN, value1};
  TableStats output_stats{};

  UNUSED_ATTRIBUTE double cost = Cost::SingleConditionSeqScanCost(table_stats.get(), &condition1, &output_stats);
  // std::cout << cost << std::endl;
  // EXPECT_EQ(output_stats.num_rows, 1000);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

} /* namespace test */
} /* namespace peloton */
