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

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/testing_executor_util.h"
#include "optimizer/stats/cost.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/value_condition.h"
#include "sql/testing_sql_util.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

using namespace optimizer;

const int N_ROW = 100;

class CostTests : public PelotonTest {};

// tablename: test
// database name: DEFAULT_DB_NAME
void CreateAndLoadTable() {
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR, salary DECIMAL);");
  for (int i = 1; i <= N_ROW; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test VALUES (" << i << ", 'name', 1.1);";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
}

std::shared_ptr<TableStats> GetTableStatsWithName(
    std::string table_name, concurrency::TransactionContext *txn) {
  auto catalog = catalog::Catalog::GetInstance();
  auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);
  auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
  oid_t db_id = database->GetOid();
  oid_t table_id = table->GetOid();
  auto stats_storage = StatsStorage::GetInstance();
  return stats_storage->GetTableStats(db_id, table_id);
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

  txn = txn_manager.BeginTransaction();
  auto table_stats = GetTableStatsWithName("test", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(table_stats, nullptr);
  EXPECT_EQ(table_stats->num_rows, N_ROW);

  // condition1: id < 1000
  type::Value value1 = type::ValueFactory::GetIntegerValue(1000);
  ValueCondition condition1{0, "id", ExpressionType::COMPARE_LESSTHAN, value1};
  std::shared_ptr<TableStats> output_stats(new TableStats{});
  double cost1 =
      Cost::SingleConditionSeqScanCost(table_stats, condition1, output_stats);
  LOG_INFO("cost for condition 1 is %f", cost1);
  EXPECT_GE(cost1, 0);
  // EXPECT_EQ(output_stats->num_rows, 1000);

  // condition2: id = 1000
  ValueCondition condition2{0, "id", ExpressionType::COMPARE_EQUAL, value1};
  output_stats->ClearColumnStats();
  double cost2 =
      Cost::SingleConditionSeqScanCost(table_stats, condition2, output_stats);
  LOG_INFO("cost for condition 2 is: %f", cost2);
  EXPECT_GE(cost2, 0);
  // EXPECT_EQ(output_stats->num_rows, 1);

  // Two seq scan cost should be the same
  EXPECT_EQ(cost1, cost2);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CostTests, ConjunctionTest) {
  std::shared_ptr<TableStats> lhs(new TableStats{8080});
  std::shared_ptr<TableStats> rhs(new TableStats{3695});
  std::shared_ptr<TableStats> output(new TableStats{});
  int n_rows = 200000;
  Cost::CombineConjunctionStats(lhs, rhs, n_rows,
                                ExpressionType::CONJUNCTION_AND, output);
  EXPECT_GE(output->num_rows, 149);
  EXPECT_LE(output->num_rows, 150);
  Cost::CombineConjunctionStats(lhs, rhs, n_rows,
                                ExpressionType::CONJUNCTION_OR, output);
  EXPECT_GE(output->num_rows, 11625);
  EXPECT_LE(output->num_rows, 11626);
}

}  // namespace test
}  // namespace peloton
