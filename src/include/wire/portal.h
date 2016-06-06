//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.h
//
// Identification: src/include/wire/portal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <tuple>

struct sqlite3_stmt;

namespace peloton {
namespace wire {

typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>>
    ResType;

// fieldinfotype: field name, oid (data type), size
typedef std::tuple<std::string, int, int> FieldInfoType;

struct Portal {
  std::string portal_name;

  // logical name of prep stmt
  std::string prep_stmt_name;

  // stores the attribute names
  std::vector<wire::FieldInfoType> tuple_desc;

  std::string query_string;

  std::string query_type;

  sqlite3_stmt *stmt;
};

}  // namespace wire
}  // namespace peloton
