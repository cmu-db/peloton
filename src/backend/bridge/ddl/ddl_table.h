//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_table.h
//
// Identification: src/backend/bridge/ddl/ddl_table.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/schema.h"
#include "backend/catalog/foreign_key.h"
#include "backend/bridge/ddl/ddl_index.h"

#include "postgres.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "postmaster/peloton.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL TABLE
//===--------------------------------------------------------------------===//

class DDLTable {
 public:
  DDLTable(const DDLTable &) = delete;
  DDLTable &operator=(const DDLTable &) = delete;
  DDLTable(DDLTable &&) = delete;
  DDLTable &operator=(DDLTable &&) = delete;

  static bool ExecCreateStmt(Node *parsetree,
                             DDL_Info* ddl_info,
                             std::vector<Node *> &parsetree_stack,
                             TransactionId txn_id);

  static bool ExecAlterTableStmt(Node *parsetree,
                                 std::vector<Node *> &parsetree_stack);

  static bool ExecDropStmt(Node *parsertree);

  static bool CreateTable(Oid relation_oid, std::string table_name,
                          std::vector<catalog::Column> column_infos,
                          catalog::Schema *schema = NULL);

  static bool AlterTable(Oid relation_oid, AlterTableStmt *Astmt);

  static bool DropTable(Oid table_oid);

  // Set reference tables to the table based on given relation oid
  static bool SetReferenceTables(std::vector<catalog::ForeignKey> &foreign_keys,
                                 oid_t relation_oid);

 private:
  static bool AddConstraint(Oid relation_oid, Constraint *constraint);
};

}  // namespace bridge
}  // namespace peloton
