//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/pessimistic_transaction_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===-----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

class PessimisticTransactionManager : public TransactionManager {
 public:
  PessimisticTransactionManager() {}

  virtual ~PessimisticTransactionManager() {}

  static PessimisticTransactionManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual void CommitTransaction();

  virtual void AbortTransaction();

};
}
}