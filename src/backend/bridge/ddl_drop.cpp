/*-------------------------------------------------------------------------
 *
 * ddl_drop.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_drop.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "backend/bridge/ddl.h"
#include "backend/bridge/bridge.h"
#include "backend/catalog/schema.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

#include "postgres.h"
#include "miscadmin.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

#include "parser/parse_node.h" // make pstate cook default
#include "parser/parse_expr.h" // cook default

namespace peloton {
namespace bridge {

static std::vector<DDL::IndexInfo> index_infos;

//===--------------------------------------------------------------------===//
// Drop Object
//===--------------------------------------------------------------------===//

/**
 * @brief Drop database.
 * @param database_oid database id.
 * @return true if we dropped the database, false otherwise
 */
bool DDL::DropDatabase(Oid database_oid){
  auto& manager = catalog::Manager::GetInstance();
  manager.DropDatabaseWithOid(database_oid);

  LOG_INFO("Dropped database with oid : %u\n", database_oid);
  return true;
}

/**
 * @brief Drop table.
 * @param table_oid Table id.
 * @return true if we dropped the table, false otherwise
 */
// FIXME :: Dependencies btw indexes and tables
bool DDL::DropTable(Oid table_oid) {

  oid_t database_oid = Bridge::GetCurrentDatabaseOid();

  if(database_oid == InvalidOid || table_oid == InvalidOid) {
    LOG_WARN("Could not drop table :: db oid : %u table oid : %u", database_oid, table_oid);
    return false;
  }

  // Get db with current database oid
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(database_oid);

  db->DropTableWithOid(table_oid);

  LOG_INFO("Dropped table with oid : %u\n", table_oid);

  return true;
}

} // namespace bridge
} // namespace peloton
