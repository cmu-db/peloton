//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl_transaction.cpp
//
// Identification: src/backend/bridge/ddl/ddl_transaction.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <cassert>

#include "backend/bridge/ddl/ddl_transaction.h"
#include "backend/common/logger.h"
#include "backend/concurrency/transaction_manager_factory.h"
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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  switch (stmt->kind) {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START: {
      LOG_TRACE("BEGIN");
      txn_manager.BeginTransaction();
    } break;

    case TRANS_STMT_COMMIT: {
      LOG_TRACE("COMMIT");
      txn_manager.CommitTransaction();
    } break;

    case TRANS_STMT_ROLLBACK: {
      LOG_TRACE("ROLLBACK");
      txn_manager.AbortTransaction();
    } break;

    default: {
      LOG_TRACE("unrecognized node type: %d", (int)nodeTag(stmt));
    } break;
  }

  return true;
}

}  // namespace bridge
}  // namespace peloton
