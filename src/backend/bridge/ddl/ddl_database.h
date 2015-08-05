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

#include "postmaster/peloton.h"

#include "postgres.h"
#include "c.h"
#include "nodes/nodes.h"

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

  static bool ExecVacuumStmt(Node *parsetree, Peloton_Status *status);

  static bool CreateDatabase(Oid database_oid);

  // TODO
  // static bool AlterDatabase( );

  // TODO
  static bool DropDatabase(Oid database_oid);
};

} // namespace bridge
} // namespace peloton
