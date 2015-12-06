//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_transaction.cpp
//
// Identification: src/backend/bridge/ddl/ddl_transaction.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
bool DDLTransaction::ExecTransactionStmt(TransactionStmt *stmt) {
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  switch (stmt->kind) {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START: {
      LOG_INFO("BEGIN");
      txn_manager.BeginTransaction();
    } break;

    case TRANS_STMT_COMMIT: {
      LOG_INFO("COMMIT");
      txn_manager.CommitTransaction();
    } break;

    case TRANS_STMT_ROLLBACK: {
      LOG_INFO("ROLLBACK");
      txn_manager.AbortTransaction();
    } break;

    default: {
      LOG_WARN("unrecognized node type: %d", (int)nodeTag(stmt));
    } break;
  }

  return true;
}

}  // namespace bridge
}  // namespace peloton
