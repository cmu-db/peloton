//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl_utils.h
//
// Identification: src/bridge/ddl/ddl_utils.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/constraint.h"
#include "catalog/foreign_key.h"
#include "catalog/schema.h"

#include "postgres.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL UTILS
//===--------------------------------------------------------------------===//

class DDLUtils {
 public:
  DDLUtils(const DDLUtils &) = delete;
  DDLUtils &operator=(const DDLUtils &) = delete;
  DDLUtils(DDLUtils &&) = delete;
  DDLUtils &operator=(DDLUtils &&) = delete;

  static void SetDefaultConstraint(ColumnDef *coldef, int column_itr,
                                   oid_t relation_oid);

  static void ParsingCreateStmt(CreateStmt *Cstmt,
                                std::vector<catalog::Column> &column_infos);
};

}  // namespace bridge
}  // namespace peloton
