//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.h
//
// Identification: src/backend/bridge/ddl/ddl_utils.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/constraint.h"
#include "backend/catalog/foreign_key.h"
#include "backend/catalog/schema.h"
#include "backend/bridge/ddl/ddl_raw_structures.h"

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

  static DDL_Info* peloton_prepare_data(Node *parsetree);

  static Database_Info* PrepareCreatedbStmt(Database_Info* parsetree);

  static Database_Info* PrepareDropdbStmt(Database_Info* parsetree);

  static void SetDefaultConstraint(ColumnDef *coldef, int column_itr,
                                   oid_t relation_oid);

  static void ParsingCreateStmt(
      CreateStmt *Cstmt, std::vector<catalog::Column> &column_infos);
};

}  // namespace bridge
}  // namespace peloton
