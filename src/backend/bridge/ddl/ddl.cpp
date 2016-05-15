//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl.cpp
//
// Identification: src/backend/bridge/ddl/ddl.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/ddl_database.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/bridge/ddl/ddl_transaction.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/common/logger.h"

#include "backend/storage/database.h"
#include "postmaster/peloton.h"

#include "postgres.h"
#include "miscadmin.h"
#include "c.h"
#include "access/xact.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Utilities.
//===--------------------------------------------------------------------===//

/**
 * @brief Process utility statement.
 * @param parsetree Parse tree
 */
void DDL::ProcessUtility(Node *parsetree) {
  ALWAYS_ASSERT(parsetree != nullptr);

  LOG_TRACE("Process Utility");

  static std::vector<Node *> parsetree_stack;

  /* When we call a backend function from different thread, the thread's stack
   * is at a different location than the main thread's stack. so it sets up
   * reference point for stack depth checking
   */
  set_stack_base();

  // Process depending on type of utility statement
  switch (nodeTag(parsetree)) {
    case T_CreatedbStmt: {
      DDLDatabase::ExecCreatedbStmt(parsetree);
      break;
    }

    case T_DropdbStmt: {
      DDLDatabase::ExecDropdbStmt(parsetree);
      break;
    }

    case T_CreateStmt:
    case T_CreateForeignTableStmt: {
      DDLTable::ExecCreateStmt(parsetree, parsetree_stack);
      break;
    }

    case T_AlterTableStmt: {
      DDLTable::ExecAlterTableStmt(parsetree, parsetree_stack);
      break;
    }

    case T_DropStmt: {
      DDLTable::ExecDropStmt(parsetree);
      break;
    }

    case T_IndexStmt: {
      DDLIndex::ExecIndexStmt(parsetree, parsetree_stack);
      break;
    }

    case T_VacuumStmt: {
      DDLDatabase::ExecVacuumStmt(parsetree);
      break;
    }

    case T_TransactionStmt: {
      TransactionStmt *stmt = (TransactionStmt *)parsetree;

      DDLTransaction::ExecTransactionStmt(stmt);
    } break;

    case T_CreateFunctionStmt: {
      LOG_TRACE("UDF function added.");
      break;
    }

    default: {
      LOG_TRACE("unrecognized node type: %d", (int)nodeTag(parsetree));
    } break;
  }
}

}  // namespace bridge
}  // namespace peloton
