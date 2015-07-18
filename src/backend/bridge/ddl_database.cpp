/*-------------------------------------------------------------------------
 *
 * ddl_database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_database.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/ddl_database.h"
#include "backend/common/logger.h"
#include "backend/storage/database.h"

#include "nodes/parsenodes.h"
#include "commands/dbcommands.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Database DDL
//===--------------------------------------------------------------------===//

/**
 * @brief Execute the create db stmt.
 * @param the parse tree
 * @return true if we handled it correctly, false otherwise
 */
bool DDLDatabase::ExecCreatedbStmt(Node* parsetree){
  CreatedbStmt* stmt = (CreatedbStmt*) parsetree;
  DDLDatabase::CreateDatabase(stmt->database_id);
  return true;
}

/**
 * @brief Execute the drop db stmt.
 * @param the parse tree
 * @return true if we handled it correctly, false otherwise
 */
bool DDLDatabase::ExecDropdbStmt(Node* parsetree){
  DropdbStmt *stmt = (DropdbStmt *) parsetree;
  Oid database_oid = get_database_oid(stmt->dbname, stmt->missing_ok);
  DDLDatabase::DropDatabase(database_oid);
  return true;
}

/**
 * @brief Create database.
 * @param database_oid database id
 * @return true if we created a database, false otherwise
 */
bool DDLDatabase::CreateDatabase(Oid database_oid){
  if(database_oid == INVALID_OID)
    return false;

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = new storage::Database(database_oid);
  manager.AddDatabase(db);

  if(db == nullptr){
    LOG_WARN("Failed to create a database (%u)", database_oid);
    return false;
  }

  LOG_INFO("Create database (%u)", database_oid);
  return true;
}


/**
 * @brief Drop database.
 * @param database_oid database id.
 * @return true if we dropped the database, false otherwise
 */
bool DDLDatabase::DropDatabase(Oid database_oid){
  auto& manager = catalog::Manager::GetInstance();
  manager.DropDatabaseWithOid(database_oid);

  LOG_INFO("Dropped database with oid : %u\n", database_oid);
  return true;
}


} // namespace bridge
} // namespace peloton

