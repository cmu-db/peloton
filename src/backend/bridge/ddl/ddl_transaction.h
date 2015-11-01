//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_transaction.h
//
// Identification: src/backend/bridge/ddl/ddl_transaction.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
  DDLTransaction &operator=(const DDLTransaction &) = delete;
  DDLTransaction(DDLTransaction &&) = delete;
  DDLTransaction &operator=(DDLTransaction &&) = delete;

  static bool ExecTransactionStmt(TransactionStmt *stmt);

 private:
};

}  // namespace bridge
}  // namespace peloton
