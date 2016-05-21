//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eager_write_txn_manager.h
//
// Identification: src/backend/concurrency/eager_write_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <queue>
#include <atomic>
#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

extern thread_local std::unordered_map<oid_t, std::unordered_set<oid_t>>
    eager_write_released_rdlock;

struct TxnList {
  txn_id_t txn_id_;
  TxnList *next;
  TxnList() : txn_id_(INVALID_TXN_ID), next(nullptr) {}
  TxnList(txn_id_t t) : txn_id_(t), next(nullptr) {}
};

struct EagerWriteTxnContext {
  //  Spinlock wait_list_lock_;
  volatile std::atomic<int> wait_for_counter_;
  std::unordered_set<txn_id_t> wait_list_;
  cid_t begin_cid_;

  EagerWriteTxnContext()
      : wait_for_counter_(0), wait_list_(), begin_cid_(INVALID_CID) {}
  ~EagerWriteTxnContext() {}
};

extern thread_local EagerWriteTxnContext *current_txn_ctx;

//===--------------------------------------------------------------------===//
// pessimistic+eager write concurrency control
//===--------------------------------------------------------------------===//
class EagerWriteTxnManager : public TransactionManager {
 public:
  EagerWriteTxnManager() : last_epoch_(0) {}
  virtual ~EagerWriteTxnManager() {}

  static EagerWriteTxnManager &GetInstance();

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void YieldOwnership(const oid_t &tile_group_id,
    const oid_t &tuple_id);

  virtual bool PerformInsert(const ItemPointer &location);

  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const ItemPointer &location);

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();
    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    EagerWriteTxnContext *txn_ctx = new EagerWriteTxnContext();
    current_txn_ctx = txn_ctx;
    txn_ctx->begin_cid_ = begin_cid;

    {
      std::lock_guard<std::mutex> lock(running_txn_map_mutex_);
      running_txn_map_[txn_id] = txn_ctx;
    }


    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();

    // Remove all reader
    RemoveReader();

    // Release all dependencies
    {
      std::lock_guard<std::mutex> lock(running_txn_map_mutex_);

      // No more dependency can be added.

      for (auto wtid : current_txn_ctx->wait_list_) {
        if (running_txn_map_.count(wtid) != 0) {
          running_txn_map_[wtid]->wait_for_counter_--;
          assert(running_txn_map_[wtid]->wait_for_counter_ >= 0);
        }
      }
      running_txn_map_.erase(txn_id);
    }

    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    delete current_txn;
    delete current_txn_ctx;
    current_txn = nullptr;
    current_txn_ctx = nullptr;
  }


 private:

  // init reserved area of a tuple
  // creator txnid | lock (for read list) | read list head
  // The txn_id could only be the cur_txn's txn id.
  void InitTupleReserved(const oid_t tile_group_id, const oid_t tuple_id) {

    auto tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(tile_group_id)->GetHeader();

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    // Hack
    *(TxnList *)(reserved_area + LIST_OFFSET) = TxnList(0);
  }

  TxnList *GetEwReaderList(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) {
    return (TxnList *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                       LIST_OFFSET);
  }

  // Use to protect the reader list, not the reader count
  void GetEwReaderLock(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Lock();
  }

  void ReleaseEwReaderLock(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Unlock();
  }

  // Add the current txn into the reader list of a tuple
  // note: should be protected in EwReaderLock
  void AddReader(storage::TileGroupHeader *tile_group_header,
                 const oid_t &tuple_id) {
    auto txn_id = current_txn->GetTransactionId();
    LOG_TRACE("Add reader %lu, tuple_id = %u", txn_id, tuple_id);

    TxnList *reader = new TxnList(txn_id);

    // GetEwReaderLock(tile_group_header, tuple_id);
    TxnList *headp = (TxnList *)(
        tile_group_header->GetReservedFieldRef(tuple_id) + LIST_OFFSET);
    reader->next = headp->next;
    headp->next = reader;
    // ReleaseEwReaderLock(tile_group_header, tuple_id);
  }

  // Remove reader from the reader list of a tuple
  void RemoveReader(storage::TileGroupHeader *tile_group_header,
                    const oid_t &tuple_id, txn_id_t txn_id) {
    LOG_TRACE("Remove reader with txn_id = %lu", txn_id);
    GetEwReaderLock(tile_group_header, tuple_id);

    TxnList *headp = (TxnList *)(
        tile_group_header->GetReservedFieldRef(tuple_id) + LIST_OFFSET);

    auto next = headp->next;
    auto prev = headp;
    bool find = false;
    while (next != nullptr) {
      if (next->txn_id_ == txn_id) {
        find = true;
        prev->next = next->next;
        delete next;
        break;
      }
      prev = next;
      next = next->next;
    }

    ReleaseEwReaderLock(tile_group_header, tuple_id);
    if (find == false) {
      assert(false);
    }
  }

  void RemoveReader();

  bool CauseDeadLock();

  std::mutex running_txn_map_mutex_;
  std::unordered_map<txn_id_t, EagerWriteTxnContext *> running_txn_map_;
  static const int LOCK_OFFSET = 0;
  static const int LIST_OFFSET = (LOCK_OFFSET + sizeof(txn_id_t));
  cid_t last_epoch_;
  cid_t last_max_commit_cid_;
};
}
}
