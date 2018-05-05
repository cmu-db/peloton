//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// what_if_index_test.cpp
//
// Identification: test/brain/what_if_index_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/what_if_index.h"
#include "common/harness.h"
#include "optimizer/stats/stats_storage.h"
#include "sql/testing_sql_util.h"
#include "planner/index_scan_plan.h"

#include "brain/testing_index_suggestion_util.h"

namespace peloton {
namespace test {

using namespace index_suggestion;

//===--------------------------------------------------------------------===//
// WhatIfIndex Tests
//===--------------------------------------------------------------------===//
class WhatIfIndexTests : public PelotonTest {
 public:
  WhatIfIndexTests() {}
};

TEST_F(WhatIfIndexTests, SingleColTest) {
  std::string table_name = "table1";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 100;

  TableSchema t({{"a", TupleValueType::INTEGER},
                 {"b", TupleValueType::INTEGER},
                 {"c", TupleValueType::INTEGER},
                 {"d", TupleValueType::INTEGER}});

  TestingIndexSuggestionUtil util(db_name);
  util.CreateAndInsertIntoTable(table_name, t, num_rows);

  // Form the query.
  std::string query("SELECT a from " + table_name +
                    " WHERE b = 100 and c = 5;");
  LOG_INFO("Query: %s", query.c_str());

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = std::shared_ptr<parser::SQLStatement>
    (stmt_list.get()->PassOutStatement(0));

  binder->BindNameToNode(sql_statement.get());
  txn_manager.CommitTransaction(txn);

  // 1. Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query without indexes: %lf", cost_without_index);
  EXPECT_NE(result->plan, nullptr);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  // 2. Get the optimized plan tree with 1 hypothetical indexes (indexes)
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"b"}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with 1 index: %lf", cost_with_index_1);
  EXPECT_NE(result->plan, nullptr);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  // 3. Get the optimized plan tree with 2 hypothetical indexes (indexes)
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"c"}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with 2 indexes: %lf", cost_with_index_2);

  EXPECT_LT(cost_with_index_1, cost_without_index);
  EXPECT_LT(cost_with_index_2, cost_without_index);
  EXPECT_NE(result->plan, nullptr);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());
}

/**
 * @brief This test checks if a hypothetical index on multiple columns
 * helps a particular query.
 */
TEST_F(WhatIfIndexTests, MultiColumnTest1) {
  std::string table_name = "dummy1";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 1000;

  TableSchema t({{"a", TupleValueType::INTEGER},
                 {"b", TupleValueType::INTEGER},
                 {"c", TupleValueType::INTEGER},
                 {"d", TupleValueType::INTEGER}});
  TestingIndexSuggestionUtil util(db_name);
  util.CreateAndInsertIntoTable(table_name, t, num_rows);

  // Form the query
  std::string query("SELECT a from " + table_name +
                    " WHERE b = 200 and c = 100;");
  LOG_INFO("Query: %s", query.c_str());

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = std::shared_ptr<parser::SQLStatement>
    (stmt_list.get()->PassOutStatement(0));

  binder->BindNameToNode(sql_statement.get());
  txn_manager.CommitTransaction(txn);

  // Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
      sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_INFO("Cost of the query without indexes {}: %lf", cost_without_index);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  // Insert hypothetical catalog objects
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"a", "c"}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  LOG_INFO("Cost of the query with index {'a', 'c'}: %lf", cost_with_index_1);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  EXPECT_EQ(cost_without_index, cost_with_index_1);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"a", "b"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  LOG_INFO("Cost of the query with index {'a', 'b'}: %lf", cost_with_index_2);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  EXPECT_EQ(cost_without_index, cost_with_index_2);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"b", "c"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_3 = result->cost;
  LOG_INFO("Cost of the query with index {'b', 'c'}: %lf", cost_with_index_3);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  EXPECT_GT(cost_without_index, cost_with_index_3);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"b"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_4 = result->cost;
  EXPECT_LE(cost_with_index_3, cost_with_index_4);

  // The cost of using one index {1} should be greater than the cost
  // of using both the indexes {1, 2} for the query.
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {'b'}: %lf", cost_with_index_4);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());
}

TEST_F(WhatIfIndexTests, MultiColumnTest2) {
  std::string table_name = "dummy1";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 1000;

  TableSchema t({{"a", TupleValueType::INTEGER},
                 {"b", TupleValueType::INTEGER},
                 {"c", TupleValueType::INTEGER},
                 {"d", TupleValueType::INTEGER},
                 {"e", TupleValueType::INTEGER},
                 {"f", TupleValueType::INTEGER}});
  TestingIndexSuggestionUtil util(db_name);
  util.CreateAndInsertIntoTable(table_name, t, num_rows);

  // Form the query.
  std::string query("SELECT a from " + table_name +
                    " WHERE b = 500 AND e = 100;");
  LOG_INFO("Query: %s", query.c_str());

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = std::shared_ptr<parser::SQLStatement>
    (stmt_list.get()->PassOutStatement(0));

  binder->BindNameToNode(sql_statement.get());
  txn_manager.CommitTransaction(txn);

  // Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
      sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_DEBUG("Cost of the query without indexes: %lf", cost_without_index);

  // Insert hypothetical catalog objects
  // Index on cols a, b, c, d, e.
  config.AddIndexObject(
      util.CreateHypotheticalIndex(table_name, {"a", "b", "c", "d", "e"}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {'a', 'b', 'c', 'd', 'e'}: %lf",
           cost_with_index_1);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_1);

  config.Clear();
  config.AddIndexObject(
      util.CreateHypotheticalIndex(table_name, {"a", "c", "d", "f"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {'a', 'c', 'd', 'f'}: %lf",
           cost_with_index_2);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_2);

  config.Clear();
  config.AddIndexObject(
      util.CreateHypotheticalIndex(table_name, {"a", "b", "d", "e"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_3 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {'a', 'b', 'd', 'e'}: %lf",
           cost_with_index_3);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_3);

  config.Clear();
  config.AddIndexObject(
      util.CreateHypotheticalIndex(table_name, {"b", "c", "e"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_4 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {'b', 'c', 'e'}: %lf",
           cost_with_index_4);
  EXPECT_GT(cost_without_index, cost_with_index_4);

  config.Clear();
  config.AddIndexObject(
      util.CreateHypotheticalIndex(table_name, {"b", "c", "d", "e"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_5 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {'b', 'c', 'd', 'e'}: %lf",
           cost_with_index_5);
  EXPECT_GT(cost_without_index, cost_with_index_5);

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"b", "e"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_6 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {'b', 'e'}: %lf", cost_with_index_6);
  EXPECT_GT(cost_without_index, cost_with_index_6);
  EXPECT_GT(cost_with_index_5, cost_with_index_6);
  EXPECT_GT(cost_with_index_4, cost_with_index_6);

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"e"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_7 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_DEBUG("Cost of the query with index {'e'} : %lf", cost_with_index_7);
  EXPECT_GT(cost_without_index, cost_with_index_7);
  EXPECT_GT(cost_with_index_7, cost_with_index_6);

  config.Clear();
  config.AddIndexObject(util.CreateHypotheticalIndex(table_name, {"b"}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_8 = result->cost;
  LOG_INFO("Cost of the query with index {'b'}: %lf", cost_with_index_8);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  EXPECT_GT(cost_without_index, cost_with_index_8);
  EXPECT_GT(cost_with_index_8, cost_with_index_6);
}

}  // namespace test
}  // namespace peloton
