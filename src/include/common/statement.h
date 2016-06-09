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

#include "common/types.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}

typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>> ResultType;

// FIELD INFO TYPE : field name, oid (data type), size
typedef std::tuple<std::string, oid_t, size_t> FieldInfoType;

class PreparedStatement {

 public:

  // logical name of statement
  std::string prepared_statement_name;

  // query string
  std::string query_string;

  // first token in query
  std::string query_type;

  // format codes of the parameters
  std::vector<int32_t> param_types;

  // schema of result tuple
  std::vector<FieldInfoType> tuple_descriptor;

  // cached plan tree
  planner::AbstractPlan *plan_tree;

};

}  // namespace peloton
