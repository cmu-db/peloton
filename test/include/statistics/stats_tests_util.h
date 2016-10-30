//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.h
//
// Identification: tests/include/statistics/stats_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_tests_util.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction.h"
#include "storage/tile_group_factory.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "index/index.h"
#include "index/index_factory.h"

#include "expression/comparison_expression.h"
#include "planner/insert_plan.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"
#include "executor/plan_executor.h"
#include "storage/data_table.h"
#include "executor/create_executor.h"

#pragma once

namespace peloton {
namespace test {

class StatsTestsUtil {
 public:
  static void ShowTable(std::string database_name, std::string table_name);

  static storage::Tuple PopulateTuple(const catalog::Schema *schema,
                                      int first_col_val, int second_col_val,
                                      int third_col_val, int fourth_col_val);

  static void CreateTable();

  static std::shared_ptr<Statement> GetInsertStmt();

  static std::shared_ptr<Statement> GetDeleteStmt();

  static std::shared_ptr<Statement> GetUpdateStmt();

  static void ParseAndPlan(Statement *statement, std::string sql);
};

}  // namespace test
}  // namespace peloton
