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
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"
#include "expression/star_expression.h"
#include "optimizer/stats/cost.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/value_condition.h"
#include "sql/testing_sql_util.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "optimizer/properties.h"

namespace peloton {
namespace test {

using namespace optimizer;

// const int N_ROW = 100;

class CostTests : public PelotonTest {};

// tablename: test
// database name: DEFAULT_DB_NAME
// void CreateAndLoadTable(const std::string& table_name = {"test"}) {
//   TestingSQLUtil::ExecuteSQLQuery(
//       "CREATE TABLE " + table_name + " (id INT PRIMARY KEY, name VARCHAR, salary DECIMAL);");
//   for (int i = 1; i <= N_ROW; i++) {
//     std::stringstream ss;
//     ss << "INSERT INTO " << table_name << " VALUES (" << i << ", 'name', 1.1);";
//     TestingSQLUtil::ExecuteSQLQuery(ss.str());
//   }
// }
//
// std::shared_ptr<PropertyColumns> GetPropertyColumns() {
//   std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
//   auto star_expr = std::shared_ptr<expression::AbstractExpression>(new expression::StarExpression());
//   cols.push_back(star_expr);
//   return std::make_shared<PropertyColumns>(cols);
// }
//
// std::shared_ptr<TableStats> GetTableStatsWithName(
//     std::string table_name, concurrency::TransactionContext *txn) {
//   auto catalog = catalog::Catalog::GetInstance();
//   auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);
//   auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
//   oid_t db_id = database->GetOid();
//   oid_t table_id = table->GetOid();
//   auto stats_storage = StatsStorage::GetInstance();
//   return stats_storage->GetTableStats(db_id, table_id);
// }
//
//
// std::shared_ptr<TableStats> GetTableStatsForJoin(
//   std::string table_name, concurrency::Transaction *txn) {
//   auto catalog = catalog::Catalog::GetInstance();
//   auto database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);
//   auto table = catalog->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
//   oid_t db_id = database->GetOid();
//   oid_t table_id = table->GetOid();
//   auto stats_storage = StatsStorage::GetInstance();
//   auto table_stats = stats_storage->GetTableStats(db_id, table_id);
//   table_stats->SetTupleSampler(std::make_shared<TupleSampler>(table));
//   auto column_prop = GetPropertyColumns();
//   return generateOutputStat(table_stats, column_prop.get(), table);
// }
//
// TEST_F(CostTests, ScanCostTest) {
//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
//   txn_manager.CommitTransaction(txn);
//
//   // create table with name test
//   CreateAndLoadTable();
//
//   // Collect stats
//   TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
//
//   txn = txn_manager.BeginTransaction();
//   auto table_stats = GetTableStatsWithName("test", txn);
//   txn_manager.CommitTransaction(txn);
//   EXPECT_NE(table_stats, nullptr);
//   EXPECT_EQ(table_stats->num_rows, N_ROW);
//
//   // condition1: id < 1000
//   type::Value value1 = type::ValueFactory::GetIntegerValue(1000);
//   ValueCondition condition1{0, "id", ExpressionType::COMPARE_LESSTHAN, value1};
//   std::shared_ptr<TableStats> output_stats(new TableStats{});
//   double cost1 =
//       Cost::SingleConditionSeqScanCost(table_stats, condition1, output_stats);
//   LOG_INFO("cost for condition 1 is %f", cost1);
//   EXPECT_GE(cost1, 0);
//   // EXPECT_EQ(output_stats->num_rows, 1000);
//
//   // condition2: id = 1000
//   ValueCondition condition2{0, "id", ExpressionType::COMPARE_EQUAL, value1};
//   output_stats->ClearColumnStats();
//   double cost2 =
//       Cost::SingleConditionSeqScanCost(table_stats, condition2, output_stats);
//   LOG_INFO("cost for condition 2 is: %f", cost2);
//   EXPECT_GE(cost2, 0);
//   // EXPECT_EQ(output_stats->num_rows, 1);
//
//   // Two seq scan cost should be the same
//   EXPECT_EQ(cost1, cost2);
//
//   // Free the database
//   txn = txn_manager.BeginTransaction();
//   catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
//   txn_manager.CommitTransaction(txn);
// }
//
// TEST_F(CostTests, ConjunctionTest) {
//   std::shared_ptr<TableStats> lhs(new TableStats{8080});
//   std::shared_ptr<TableStats> rhs(new TableStats{3695});
//   std::shared_ptr<TableStats> output(new TableStats{});
//   int n_rows = 200000;
//   Cost::CombineConjunctionStats(lhs, rhs, n_rows,
//                                 ExpressionType::CONJUNCTION_AND, output);
//   EXPECT_GE(output->num_rows, 149);
//   EXPECT_LE(output->num_rows, 150);
//   Cost::CombineConjunctionStats(lhs, rhs, n_rows,
//                                 ExpressionType::CONJUNCTION_OR, output);
//   EXPECT_GE(output->num_rows, 11625);
//   EXPECT_LE(output->num_rows, 11626);
// }
//
// TEST_F(CostTests, JoinTest) {
//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
//   txn_manager.CommitTransaction(txn);
//
//   // create table with name test1 and test2
//   CreateAndLoadTable("test1");
//   CreateAndLoadTable("test2");
//
//   // Collect stats
//   TestingSQLUtil::ExecuteSQLQuery("ANALYZE test1");
//   TestingSQLUtil::ExecuteSQLQuery("ANALYZE test2");
//
//   txn = txn_manager.BeginTransaction();
//   auto left_table_stats = GetTableStatsForJoin("test1", txn);
//   auto right_table_stats = GetTableStatsForJoin("test2", txn);
//
//   txn_manager.CommitTransaction(txn);
//
//   auto expr1 = new expression::TupleValueExpression("id", "test1");
//   auto expr2 = new expression::TupleValueExpression("id", "test2");
//   auto predicate = std::shared_ptr<expression::AbstractExpression>(expression::ExpressionUtil::ComparisonFactory(
//     ExpressionType::COMPARE_EQUAL, expr1, expr2));
//
//
//   auto column_prop = GetPropertyColumns();
//
//   std::shared_ptr<TableStats> output_stats = generateOutputStatFromTwoTable(
//     left_table_stats,
//     right_table_stats,
//     column_prop.get());
//
//   double cost = Cost::NLJoinCost(left_table_stats, right_table_stats, output_stats, predicate, JoinType::INNER, true);
//   LOG_INFO("Estimated output size %lu", output_stats->num_rows);
//   EXPECT_EQ(cost, 100);
//   EXPECT_EQ(output_stats->GetSampler()->GetSampledTuples().size(), 100);
//
//
//
// }
}  // namespace test
}  // namespace peloton
