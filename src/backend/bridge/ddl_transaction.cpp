/*-------------------------------------------------------------------------
 *
 * ddl_transaction.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_transaction.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/ddl_transaction.h"
#include "backend/common/logger.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Transaction DDL
//===--------------------------------------------------------------------===//

/**
 * @brief Execute the transaction stmt.
 * @param the statement
 * @return true if we handled it correctly, false otherwise
 */
bool DDLTransaction::ExecTransactionStmt(TransactionStmt* stmt){

  auto& manager = catalog::Manager::GetInstance();

  switch (stmt->kind)
  {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START:
    {
      LOG_INFO("begin");
    }
    break;

    case TRANS_STMT_COMMIT:
    {
      LOG_INFO("commit");
    }
    break;

    case TRANS_STMT_ROLLBACK:
    {
      LOG_INFO("rollback ");
    }
    break;

    default: {
      LOG_WARN("unrecognized node type: %d", (int) nodeTag(stmt));
    }
    break;
  }

  return true;
}


} // namespace bridge
} // namespace peloton
