/*-------------------------------------------------------------------------
 *
 * ddl_transaction.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_transaction.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/manager.h"

#include "postgres.h"
#include "c.h"
#include "nodes/parsenodes.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL TRANSACTION
//===--------------------------------------------------------------------===//

class DDLTransaction {

 public:
  DDLTransaction(const DDLTransaction &) = delete;
  DDLTransaction& operator=(const DDLTransaction &) = delete;
  DDLTransaction(DDLTransaction &&) = delete;
  DDLTransaction& operator=(DDLTransaction &&) = delete;

  static bool ExecTransactionStmt(TransactionStmt* stmt,
                                  TransactionId txn_id);

 private:

};

} // namespace bridge
} // namespace peloton
