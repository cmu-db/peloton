/*-------------------------------------------------------------------------
 *
 * ddl_alter.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_alter.cpp
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

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Alter Object
//===--------------------------------------------------------------------===//

/**
 * @brief AlterTable with given AlterTableStmt
 * @param relation_oid relation oid 
 * @param Astmt AlterTableStmt 
 * @return true if we alter the table successfully, false otherwise
 */
bool DDL::AlterTable(Oid relation_oid, AlterTableStmt* Astmt){

  ListCell* lcmd;
  foreach(lcmd, Astmt->cmds)
  {
    AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

    switch (cmd->subtype){
      //case AT_AddColumn:  /* add column */
      //case AT_DropColumn:  /* drop column */

      case AT_AddConstraint:	/* ADD CONSTRAINT */
      {
        bool status = bridge::DDL::AddConstraint(relation_oid, (Constraint*) cmd->def);

        if(status == false){
          LOG_WARN("Failed to add constraint");
        }
        break;
      }
      default:
        break;
    }
  }

  LOG_INFO("Alter the table (%u)\n", relation_oid);
  return true;
}

} // namespace bridge
} // namespace peloton
