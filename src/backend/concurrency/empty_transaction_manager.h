//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/empty_transaction_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

// for performance measurement only.
class EmptyTransactionManager : public TransactionManager {
 public:
  EmptyTransactionManager() {}

  virtual ~EmptyTransactionManager() {}

  static EmptyTransactionManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool CommitTransaction();

  virtual void AbortTransaction();
};
}
}