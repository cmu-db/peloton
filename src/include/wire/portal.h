//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.h
//
// Identification: src/wire/portal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "wire/marshal.h"
#include "wire/sqlite.h"

namespace peloton {
namespace wire {

struct Portal {
  std::string portal_name;
  std::string prep_stmt_name; // logical name of prep stmt
  std::vector<wire::FieldInfoType> rowdesc; // stores the attribute names
  std::string query_string;
  std::string query_type;
  sqlite3_stmt *stmt;
};

} // namespace wire
} //namespace peloton
