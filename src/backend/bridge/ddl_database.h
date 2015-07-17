/*-------------------------------------------------------------------------
 *
 * ddl_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "postgres.h"
#include "c.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL DATABASE
//===--------------------------------------------------------------------===//

class DDLDatabase {

 public:
  DDLDatabase(const DDLDatabase &) = delete;
  DDLDatabase& operator=(const DDLDatabase &) = delete;
  DDLDatabase(DDLDatabase &&) = delete;
  DDLDatabase& operator=(DDLDatabase &&) = delete;

  static bool CreateDatabase(Oid database_oid);

  // TODO
  //static bool AlterDatabase( );

  // TODO
  static bool DropDatabase(Oid database_oid);

};

} // namespace bridge
} // namespace peloton
