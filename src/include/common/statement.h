//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.h
//
// Identification: src/include/common/statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

namespace peloton {

class PreparedStatement {

};

class Statement {

 public:

  // logical name of statement
  std::string stmt_name;

  // pointer to prepared statement
  PreparedStatement *sql_stmt;

  // query string
  std::string query_string;

  // first token in query
  std::string query_type;

  // format codes of the parameters
  std::vector<int32_t> param_types;

};

}  // namespace peloton
