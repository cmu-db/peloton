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

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

class SQLTestsUtil {
 public:
  // Show the content in the specific table in the specific database
  static void ShowTable(std::string database_name, std::string table_name);

  // Execute a SQL query end-to-end
  static void ExecuteSQLQuery(const std::string statement_name,
                              const std::string query_string);
};
}  // namespace test
}  // namespace peloton
