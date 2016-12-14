//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.h
//
// Identification: test/include/sql/sql_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/statement.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

class SQLTestsUtil {
 public:
  // Show the content in the specific table in the specific database
  // Note: In order to see the content from the command line, you have to
  // turn-on LOG_TRACE.
  static void ShowTable(std::string database_name, std::string table_name);

  // Execute a SQL query end-to-end
  static Result ExecuteSQLQuery(const std::string query,
                                std::vector<ResultType> &result,
                                std::vector<FieldInfoType> &tuple_descriptor,
                                int &rows_changed, std::string &error_message);

  // A simpler wrapper around ExecuteSQLQuery
  static Result ExecuteSQLQuery(const std::string query,
                                std::vector<ResultType> &result);

  // A another simpler wrapper around ExecuteSQLQuery
  static Result ExecuteSQLQuery(const std::string query);

  // Execute a SQL query end-to-end with the specific optimizer
  static Result ExecuteSQLQueryWithOptimizer(
      const std::string query_string, std::vector<ResultType> &result,
      std::vector<FieldInfoType> &tuple_descriptor, int &rows_changed,
      std::string &error_message);
}  // namespace test
}  // namespace peloton
