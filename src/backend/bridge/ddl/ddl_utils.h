/*-------------------------------------------------------------------------
 *
 * ddl_utils.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_utils.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/constraint.h"
#include "backend/catalog/foreign_key.h"
#include "backend/catalog/schema.h"

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
  DDLUtils(const DDLUtils&) = delete;
  DDLUtils& operator=(const DDLUtils&) = delete;
  DDLUtils(DDLUtils&&) = delete;
  DDLUtils& operator=(DDLUtils&&) = delete;

  static void peloton_prepare_data(Node* parsetree);

  static void SetDefaultConstraint(ColumnDef* coldef, 
                                   int column_itr, 
                                   Relation relation);

  // Parse IndexStmt and construct ColumnInfo and ReferenceTableInfos
  static void ParsingCreateStmt(
      CreateStmt* Cstmt, std::vector<catalog::Column>& column_infos,
      std::vector<catalog::ForeignKey>& reference_table_infos);
};

}  // namespace bridge
}  // namespace peloton
