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

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Database DDL
//===--------------------------------------------------------------------===//

/**
 * @brief Create database.
 * @param database_oid database id
 * @return true if we created a database, false otherwise
 */
bool DDLDatabase::CreateDatabase(Oid database_oid){

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(Bridge::Bridge::GetCurrentDatabaseOid());

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

