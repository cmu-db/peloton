//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_database.h
//
// Identification: src/backend/bridge/ddl/ddl_database.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"

#include "nodes/nodes.h"

struct peloton_status;

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL DATABASE
//===--------------------------------------------------------------------===//

class DDLDatabase {
 public:
  DDLDatabase(const DDLDatabase &) = delete;
  DDLDatabase &operator=(const DDLDatabase &) = delete;
  DDLDatabase(DDLDatabase &&) = delete;
  DDLDatabase &operator=(DDLDatabase &&) = delete;

  static bool ExecCreatedbStmt(Node *parsetree);

  static bool ExecDropdbStmt(Node *parsetree);

  static bool ExecVacuumStmt(Node *parsetree);

  static bool CreateDatabase(oid_t database_oid);

  // TODO
  // static bool AlterDatabase( );

  // TODO
  static bool DropDatabase(oid_t database_oid);
};

}  // namespace bridge
}  // namespace peloton
