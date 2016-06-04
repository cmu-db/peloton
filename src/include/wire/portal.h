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

#include "sqlite.h"
#include <string>
#include <vector>
#include "marshal.h"

namespace peloton {
namespace wire {

struct Portal {
  std::string portal_name;
  std::string prep_stmt_name; // logical name of prep stmt
  std::vector<wiredb::FieldInfoType> rowdesc; // stores the attribute names
  std::string query_string;
  std::string query_type;
  sqlite3_stmt *stmt;
};

} // namespace wire
} //namespace peloton
