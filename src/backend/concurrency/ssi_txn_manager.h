//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rowo_txn_manager.h
//
// Identification: src/backend/concurrency/ssi_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {


struct SsiTxnContext {
  SsiTxnContext(Transaction *t) : transaction_(t), in_conflict_(false), out_conflict_(false) {}
  Transaction *transaction_;
  bool in_conflict_;
  bool out_conflict_;
};

struct ReadList {
  txn_id_t txnId;
  ReadList *next;
  ReadList() : txnId(INVALID_TXN_ID), next(nullptr) {}
  ReadList(txn_id_t t) : txnId(t), next(nullptr) {}
};


class SsiTxnManager : public TransactionManager {
 public:
  SsiTxnManager() {}

  virtual ~SsiTxnManager() {}

  static SsiTxnManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id);

  virtual bool IsAccessable(storage::TileGroup *tile_group,
                            const oid_t &tuple_id);

  virtual bool AcquireTuple(storage::TileGroup *tile_group,
                            const oid_t &physical_tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void SetDeleteVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void SetUpdateVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual Transaction *BeginTransaction() {
    Transaction *txn = TransactionManager::BeginTransaction();
    {
      std::lock_guard<std::mutex> lock(running_txns_mutex_);
      assert(running_txns_.find(txn->GetTransactionId()) == running_txns_.end());
      //running_txns_[txn->GetTransactionId()] = SsiTxnContext(txn);
      running_txns_.insert(std::make_pair(txn->GetTransactionId(), SsiTxnContext(txn)));
    }
    return txn;
  }

  virtual void EndTransaction() {}

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

private:
  std::mutex running_txns_mutex_;
  std::map<txn_id_t, SsiTxnContext> running_txns_;

  // init reserved area of a tuple
  // creator | lock | read list
  void InitTupleReserved(const txn_id_t t, const oid_t tile_group_id, const oid_t tuple_id);
  inline txn_id_t * GetCreatorAddr(const char *reserved_area) { return ((txn_id_t *)(reserved_area + CREATOR_OFFSET)); }
  inline txn_id_t * GetLockAddr(const char *reserved_area) { return ((txn_id_t *)(reserved_area + LOCK_OFFSET)); }
  inline ReadList ** GetListAddr(const char *reserved_area) { return ((ReadList **)(reserved_area + LIST_OFFSET)); }
  char *GetReservedAreaAddr(const oid_t tile_group_id, const oid_t tuple_id);
  bool GetReadLock(txn_id_t *lock_addr, txn_id_t who);
  bool ReleaseReadLock(txn_id_t *lock_addr, txn_id_t holder);
  void RemoveReader(txn_id_t txn_id);

  static const int CREATOR_OFFSET = 0;
  static const int LOCK_OFFSET = (CREATOR_OFFSET + sizeof(txn_id_t));
  static const int LIST_OFFSET = (LOCK_OFFSET + sizeof(txn_id_t));
};
}
}
