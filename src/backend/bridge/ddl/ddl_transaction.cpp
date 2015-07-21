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

#include <iostream>
#include <cassert>

#include "backend/bridge/ddl/ddl_transaction.h"
#include "backend/common/logger.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"

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
bool DDLTransaction::ExecTransactionStmt(TransactionStmt* stmt,
                                         TransactionId txn_id){

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  LOG_TRACE("Transaction ID :: %u \n", txn_id);

  switch (stmt->kind)
  {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START:
    {
      LOG_TRACE("BEGIN");
      auto txn = txn_manager.StartPGTransaction(txn_id);
      assert(txn);
      LOG_TRACE("Started new peloton txn : %lu \n", txn->GetTransactionId());
    }
    break;

    case TRANS_STMT_COMMIT:
    {
      LOG_TRACE("COMMIT");
      auto txn = txn_manager.GetPGTransaction(txn_id);
      assert(txn);
      LOG_TRACE("Committing peloton txn : %lu \n", txn->GetTransactionId());
      txn_manager.CommitTransaction(txn);
    }
    break;

    case TRANS_STMT_ROLLBACK:
    {
      LOG_TRACE("ROLLBACK");
      auto txn = txn_manager.GetPGTransaction(txn_id);
      assert(txn);
      LOG_TRACE("Aborting peloton txn : %lu \n", txn->GetTransactionId());
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
