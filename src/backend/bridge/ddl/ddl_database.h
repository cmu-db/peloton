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

#include "backend/bridge/ddl/ddl_raw_structures.h"
#include "backend/common/types.h"

struct Peloton_Status;

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

  static bool ExecDropdbStmt(Node *parsetree, DDL_Info* ddl_info);

  static bool ExecVacuumStmt(Node *parsetree, Peloton_Status *status);

  static bool CreateDatabase(oid_t database_oid);

  // TODO
  // static bool AlterDatabase( );

  // TODO
  static bool DropDatabase(oid_t database_oid);
};

}  // namespace bridge
}  // namespace peloton
