//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.h
//
// Identification: src/include/common/portal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <tuple>

#include "common/statement.h"

namespace peloton {

typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>>
    ResType;

// fieldinfotype: field name, oid (data type), size
typedef std::tuple<std::string, int, int> FieldInfoType;

class Portal {

public:

  std::string portal_name;

  // logical name of prep stmt
  std::string prep_stmt_name;

  // stores the attribute names
  std::vector<FieldInfoType> tuple_desc;

  std::string query_string;

  std::string query_type;

  PreparedStatement *stmt;
};

}  // namespace peloton
