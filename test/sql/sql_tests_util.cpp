//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.cpp
//
// Identification: test/sql/sql_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/logger.h"
#include "executor/plan_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "tcop/tcop.h"

#include "sql/sql_tests_util.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

// Show the content in the specific table in the specific database
// Note: In order to see the content from the command line, you have to
// turn-on LOG_TRACE.
void SQLTestsUtil::ShowTable(std::string database_name,
                             std::string table_name) {
  ExecuteSQLQuery("SELECT * FROM " + database_name + "." + table_name);
}

// Execute a SQL query end-to-end
Result SQLTestsUtil::ExecuteSQLQuery(
    const std::string query, optimizer::AbstractOptimizer &optimizer,
    std::vector<ResultType> &result,
    std::vector<FieldInfoType> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_INFO("Query: %s", query.c_str());
  auto status = tcop::TrafficCop::GetInstance().ExecuteStatement(
      query, optimizer, result, tuple_descriptor, rows_changed, error_message);
  LOG_TRACE("Statement executed. Result: %d", status);
  return status;
}

Result SQLTestsUtil::ExecuteSQLQuery(const std::string query,
                                     std::vector<ResultType> &result) {
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  // execute the query using tcop
  auto status = tcop::TrafficCop::GetInstance().ExecuteStatement(
      query, optimizer, result, tuple_descriptor, rows_affected, error_message);

  return status;
}

Result SQLTestsUtil::ExecuteSQLQuery(const std::string query) {
  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  // execute the query using tcop
  auto status = tcop::TrafficCop::GetInstance().ExecuteStatement(
      query, optimizer, result, tuple_descriptor, rows_affected, error_message);

  return status;
}

}  // namespace test
}  // namespace peloton
