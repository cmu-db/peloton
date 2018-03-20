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

#include "../concurrency/testing_transaction_util.h"
#include "common/harness.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_context.h"
#include "storage/tile_group_factory.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "index/index.h"
#include "index/index_factory.h"

#include "expression/comparison_expression.h"
#include "planner/insert_plan.h"
#include "parser/postgresparser.h"

#include "executor/plan_executor.h"
#include "storage/data_table.h"
#include "executor/create_executor.h"

#pragma once

namespace peloton {
namespace test {

class TestingStatsUtil {
 public:
  static void ShowTable(std::string database_name, std::string table_name);

  static storage::Tuple PopulateTuple(const catalog::Schema *schema,
                                      int first_col_val, int second_col_val,
                                      int third_col_val, int fourth_col_val);

  static void CreateTable(bool has_primary_key = true);

  static std::shared_ptr<stats::QueryMetric::QueryParams> GetQueryParams(
      std::shared_ptr<uchar> &type_buf, std::shared_ptr<uchar> &format_buf,
      std::shared_ptr<uchar> &val_buf);

  static std::shared_ptr<Statement> GetInsertStmt(int id = 1,
                                                  std::string val = "hello");

  static std::shared_ptr<Statement> GetDeleteStmt();

  static std::shared_ptr<Statement> GetUpdateStmt();

  static void ParseAndPlan(Statement *statement, std::string sql);
};

}  // namespace test
}  // namespace peloton
