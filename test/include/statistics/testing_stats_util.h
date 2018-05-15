//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.h
//
// Identification: tests/include/statistics/stats_tests_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "common/harness.h"
#include "common/internal_types.h"
#include "common/statement.h"
#include "../concurrency/testing_transaction_util.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_context.h"
#include "executor/create_executor.h"
#include "executor/plan_executor.h"
#include "expression/comparison_expression.h"
#include "index/index.h"
#include "index/index_factory.h"
#include "parser/postgresparser.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_factory.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class TestingStatsUtil {
 public:
  static void CreateTable(bool has_primary_key = true);

  static std::shared_ptr<Statement> GetInsertStmt(int id = 1,
                                                  std::string val = "hello");

  static void ParseAndPlan(Statement *statement, std::string sql);

  static int AggregateCounts();

  static void Initialize();

  static std::pair<oid_t, oid_t> GetDbTableID(const std::string &table_name);

  static std::pair<oid_t, oid_t> GetDbIndexID(const std::string &table_name);
};

class StatsWorkload {
 public:
  inline void DoQueries() {
    for (auto &query : queries_) {
      EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery(query));
    }
  }

  inline void AddQuery(std::string query) { queries_.push_back(query); }

  inline StatsWorkload MakeCopy() {
    StatsWorkload copy;
    copy.queries_ = this->queries_;
    return copy;
  }

 private:
  std::vector<std::string> queries_;
};

}  // namespace test
}  // namespace peloton
