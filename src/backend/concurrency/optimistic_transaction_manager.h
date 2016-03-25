//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/optimistic_transaction_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

class OptimisticTransactionManager : public TransactionManager {
 public:
  OptimisticTransactionManager() {}

  virtual ~OptimisticTransactionManager() {}

  static OptimisticTransactionManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool RecordRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool RecordWrite(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool RecordInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool RecordDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetVisibilityForCurrentTxn(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

};
}
}