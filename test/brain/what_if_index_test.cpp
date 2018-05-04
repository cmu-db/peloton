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
#include "brain/index_selection_util.h"
#include "catalog/index_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"
#include "planner/index_scan_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// WhatIfIndex Tests
//===--------------------------------------------------------------------===//

class WhatIfIndexTests : public PelotonTest {
 private:
  std::string database_name;

 public:
  WhatIfIndexTests() {}

  // Create a new database
  void CreateDatabase(std::string db_name) {
    database_name = db_name;
    std::string create_db_str = "CREATE DATABASE " + db_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_db_str);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(std::string table_name) {
    std::string create_str =
        "CREATE TABLE " + table_name + "(a INT, b INT, c INT, d INT, e INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  // Inserts a given number of tuples with increasing values into the table.
  void InsertIntoTable(std::string table_name, int no_of_tuples) {
    // Insert tuples into table
    for (int i = 0; i < no_of_tuples; i++) {
      std::ostringstream oss;
      oss << "INSERT INTO " << table_name << " VALUES (" << i << "," << i + 1
          << "," << i + 2 << "," << i + 3 << "," << i + 4 << ");";
      TestingSQLUtil::ExecuteSQLQuery(oss.str());
    }
  }

  void DropTable(std::string table_name) {
    std::string create_str = "DROP TABLE " + table_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void DropDatabase(std::string db_name) {
    std::string create_str = "DROP DATABASE " + db_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  // Generates table stats to perform what-if index queries.
  void GenerateTableStats() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    optimizer::StatsStorage *stats_storage =
        optimizer::StatsStorage::GetInstance();
    ResultType result = stats_storage->AnalyzeStatsForAllTables(txn);
    PELOTON_ASSERT(result == ResultType::SUCCESS);
    (void)result;
    txn_manager.CommitTransaction(txn);
  }

  // Create a what-if index on the columns at the given
  // offset of the table.
  std::shared_ptr<brain::IndexObject> CreateHypotheticalIndex(
      std::string table_name, std::vector<oid_t> col_offsets) {
    // We need transaction to get table object.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Get the existing table so that we can find its oid and the cols oids.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name, table_name, txn);
    auto col_obj_pairs = table_object->GetColumnObjects();

    std::vector<oid_t> cols;
    auto database_oid = table_object->GetDatabaseOid();
    auto table_oid = table_object->GetTableOid();

    // Find the column oids.
    for (auto it = col_obj_pairs.begin(); it != col_obj_pairs.end(); it++) {
      LOG_DEBUG("Table id: %d, Column id: %d, Offset: %d, Name: %s",
                it->second->GetTableOid(), it->second->GetColumnId(),
                it->second->GetColumnOffset(),
                it->second->GetColumnName().c_str());
      for (auto given_col : col_offsets) {
        if (given_col == it->second->GetColumnId()) {
          cols.push_back(it->second->GetColumnId());
        }
      }
    }
    PELOTON_ASSERT(cols.size() == col_offsets.size());

    auto obj_ptr = new brain::IndexObject(database_oid, table_oid, cols);
    auto index_obj = std::shared_ptr<brain::IndexObject>(obj_ptr);

    txn_manager.CommitTransaction(txn);
    return index_obj;
  }
};

TEST_F(WhatIfIndexTests, SingleColTest) {
  std::string table_name = "dummy_table_whatif";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 10000;

  CreateDatabase(db_name);

  CreateTable(table_name);

  InsertIntoTable(table_name, num_rows);

  GenerateTableStats();

  // Form the query.
  std::string query("SELECT a from " + table_name +
                    " WHERE b = 100 and c = 5;");

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = stmt_list.get()->GetStatement(0);

  binder->BindNameToNode(sql_statement);
  txn_manager.CommitTransaction(txn);

  // 1. Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
      sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_INFO("Cost of the query without indexes: %lf", cost_without_index);
  EXPECT_NE(result->plan, nullptr);
  LOG_INFO("%s", result->plan->GetInfo().c_str());

  // 2. Get the optimized plan tree with 1 hypothetical indexes (indexes)
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  LOG_INFO("Cost of the query with 1 index: %lf", cost_with_index_1);
  EXPECT_NE(result->plan, nullptr);
  LOG_INFO("%s", result->plan->GetInfo().c_str());

  // 3. Get the optimized plan tree with 2 hypothetical indexes (indexes)
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {2}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  LOG_INFO("Cost of the query with 2 indexes: %lf", cost_with_index_2);

  EXPECT_LT(cost_with_index_1, cost_without_index);
  EXPECT_LT(cost_with_index_2, cost_with_index_1);
  EXPECT_NE(result->plan, nullptr);
  LOG_INFO("%s", result->plan->GetInfo().c_str());

  DropTable(table_name);
  DropDatabase(db_name);
}

/**
 * @brief This test checks if a hypothetical index on multiple columns
 * helps a particular query.
 */
TEST_F(WhatIfIndexTests, MultiColumnTest1) {
  std::string table_name = "dummy_table_whatif";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 1000;

  CreateDatabase(db_name);

  CreateTable(table_name);

  InsertIntoTable(table_name, num_rows);

  GenerateTableStats();

  // Form the query.
  std::string query("SELECT a from " + table_name +
                    " WHERE b = 200 and c = 100;");

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = stmt_list.get()->GetStatement(0);

  binder->BindNameToNode(sql_statement);
  txn_manager.CommitTransaction(txn);

  // Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
      sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_INFO("Cost of the query without indexes: %lf", cost_without_index);
  LOG_INFO("%s", result->plan->GetInfo().c_str());

  // Insert hypothetical catalog objects
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {0, 2}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  LOG_INFO("Cost of the query with index {0, 2}: %lf", cost_with_index_1);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  EXPECT_EQ(cost_without_index, cost_with_index_1);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {0, 1}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  LOG_INFO("Cost of the query with index {0, 1}: %lf", cost_with_index_2);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  EXPECT_EQ(cost_without_index, cost_with_index_2);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 2}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_3 = result->cost;
  LOG_INFO("Cost of the query with index {1, 2}: %lf", cost_with_index_3);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  EXPECT_GT(cost_without_index, cost_with_index_3);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_4 = result->cost;
  EXPECT_LE(cost_with_index_3, cost_with_index_4);

  // The cost of using one index {1} should be greater than the cost
  // of using both the indexes {1, 2} for the query.
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {1}: %lf", cost_with_index_4);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  DropTable(table_name);
  DropDatabase(db_name);
}

TEST_F(WhatIfIndexTests, MultiColumnTest2) {
  std::string table_name = "dummy_table_whatif";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 1000;

  CreateDatabase(db_name);

  CreateTable(table_name);

  InsertIntoTable(table_name, num_rows);

  GenerateTableStats();

  // Form the query.
  std::string query("SELECT a from " + table_name + " WHERE b = 500 AND e = 100;");

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = stmt_list.get()->GetStatement(0);

  binder->BindNameToNode(sql_statement);
  txn_manager.CommitTransaction(txn);

  // Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
      sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_DEBUG("Cost of the query without indexes: %lf", cost_without_index);

  // Insert hypothetical catalog objects
  // Index on cols a, b, c, d, e.
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {0, 1, 2, 3, 4}));

  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {0, 1, 2, 3, 4}: %lf", cost_with_index_1);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_1);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {0, 2, 3, 5}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {0, 2, 3, 5}: %lf", cost_with_index_2);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_2);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {0, 1, 3, 4}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_3 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::SEQSCAN);
  LOG_INFO("Cost of the query with index {0, 1, 3, 4}: %lf", cost_with_index_3);
  EXPECT_DOUBLE_EQ(cost_without_index, cost_with_index_3);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 3, 4}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_4 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {1, 3, 4}: %lf", cost_with_index_4);
  EXPECT_GT(cost_without_index, cost_with_index_4);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 2, 3, 4}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_5 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {1, 2, 3, 4}: %lf", cost_with_index_5);
  EXPECT_GT(cost_without_index, cost_with_index_5);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 4}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_6 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_INFO("Cost of the query with index {1, 4}: %lf", cost_with_index_6);
  EXPECT_GT(cost_without_index, cost_with_index_6);
  EXPECT_GT(cost_with_index_5, cost_with_index_6);
  EXPECT_GT(cost_with_index_4, cost_with_index_6);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {4}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_7 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  LOG_DEBUG("Cost of the query with index {4} : %lf", cost_with_index_7);
  EXPECT_GT(cost_without_index, cost_with_index_7);
  EXPECT_GT(cost_with_index_7, cost_with_index_6);

  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_8 = result->cost;
  LOG_INFO("Cost of the query with index {1}: %lf", cost_with_index_8);
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  EXPECT_GT(cost_without_index, cost_with_index_8);
  EXPECT_GT(cost_with_index_8, cost_with_index_6);

  DropTable(table_name);
  DropDatabase(db_name);
}


/**
 * @brief If given a set of hypothetical indexes, this checks
 * if the query optimizer picks the lowest cost one for the given
 * query.
 *
 * for example:
 * the query is SELECT * from table where b = 500 and d = 100
 * and the hypothetical indexes are {a}, {b}, {b, c}, {b, d}, {d}
 * validate if the optimizer picks {b, d} over {b} or {d}
 */
TEST_F(WhatIfIndexTests, MultiColumnTest3) {
  std::string table_name = "dummy_table_whatif";
  std::string db_name = DEFAULT_DB_NAME;
  int num_rows = 5000;

  // Setup the database.
  CreateDatabase(db_name);
  CreateTable(table_name);
  InsertIntoTable(table_name, num_rows);
  GenerateTableStats();

  // Form the query.
  std::string query("SELECT a from " + table_name + " WHERE b = 500 AND d = 100 AND e = 100;");

  brain::IndexConfiguration config;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
    parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));

  // Get the first statement.
  auto sql_statement = stmt_list.get()->GetStatement(0);

  binder->BindNameToNode(sql_statement);
  txn_manager.CommitTransaction(txn);

  // Get the optimized plan tree without the indexes (sequential scan)
  auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
    sql_statement, config, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_INFO("Cost of the query without indexes: %lf", cost_without_index);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  // Optimizer will pick the best among these.
  config.Clear();
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 5}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {4}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {4, 5}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 3}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {1, 3, 4}));
  config.AddIndexObject(CreateHypotheticalIndex(table_name, {5}));
  result = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                      DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  EXPECT_EQ(result->plan->GetPlanNodeType(), PlanNodeType::INDEXSCAN);
  EXPECT_GT(cost_without_index, cost_with_index_1);

  LOG_INFO("Cost of the query with index: %lf", cost_with_index_1);
  LOG_DEBUG("%s", result->plan->GetInfo().c_str());

  // Check the columns
  auto index_scan_plan = static_cast<planner::IndexScanPlan *>(result->plan.get());
  EXPECT_EQ(index_scan_plan->GetKeyColumnIds().size(), 3);
  EXPECT_EQ(index_scan_plan->GetKeyColumnIds()[0], 1);
  EXPECT_EQ(index_scan_plan->GetKeyColumnIds()[1], 3);
  EXPECT_EQ(index_scan_plan->GetKeyColumnIds()[2], 4);

  DropTable(table_name);
  DropDatabase(db_name);
}

}  // namespace test
}  // namespace peloton
