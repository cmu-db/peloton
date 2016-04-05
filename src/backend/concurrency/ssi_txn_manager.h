//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssi_txn_manager.h
//
// Identification: src/backend/concurrency/ssi_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/catalog/manager.h"

namespace peloton {
namespace concurrency {

struct SsiTxnContext {
  SsiTxnContext(Transaction *t)
      : transaction_(t), in_conflict_(false), out_conflict_(false) {}
  Transaction *transaction_;
  bool in_conflict_;
  bool out_conflict_;
};

struct ReadList {
  txn_id_t txn_id;
  ReadList *next;
  ReadList() : txn_id(INVALID_TXN_ID), next(nullptr) {}
  ReadList(txn_id_t t) : txn_id(t), next(nullptr) {}
};

class SsiTxnManager : public TransactionManager {
 public:
  SsiTxnManager() {}

  virtual ~SsiTxnManager() {}

  static SsiTxnManager &GetInstance();

  virtual bool IsVisible(
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

  virtual void SetOwnership(const oid_t &tile_group_id, const oid_t &tuple_id);
  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual Transaction *BeginTransaction() {
    Transaction *txn = TransactionManager::BeginTransaction();
    {
      std::lock_guard<std::mutex> lock(txn_table_mutex_);
      assert(txn_table_.find(txn->GetTransactionId()) == txn_table_.end());
      txn_table_.insert(
          std::make_pair(txn->GetTransactionId(), SsiTxnContext(txn)));
    }
    return txn;
  }

  virtual void EndTransaction() {}

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

 private:
  std::mutex txn_table_mutex_;
  std::map<txn_id_t, SsiTxnContext> txn_table_;

  // init reserved area of a tuple
  // creator txnid | lock (for read list) | read list head
  // The txn_id could only be the cur_txn's txn id.
  void InitTupleReserved(const txn_id_t txn_id, const oid_t tile_group_id,
                         const oid_t tuple_id) {
    LOG_INFO("init reserved txn %ld, group %ld tid %ld", txn_id, tile_group_id,
             tuple_id);

    auto tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(tile_group_id)->GetHeader();

    assert(tile_group_header->GetTransactionId(tuple_id) == txn_id);
    assert(current_txn->GetTransactionId() == txn_id);

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    *(txn_id_t *)(reserved_area + CREATOR_OFFSET) = txn_id;
    *(txn_id_t *)(reserved_area + LOCK_OFFSET) = INITIAL_TXN_ID;
    *(ReadList **)(reserved_area + LIST_OFFSET) = nullptr;
  }

  // Get creator of a tuple
  inline txn_id_t GetCreatorTxnId(storage::TileGroup *tile_group,
                                  const oid_t &tuple_id) {
    return *(txn_id_t *)(tile_group->GetHeader()->GetReservedFieldRef(
        tuple_id) + CREATOR_OFFSET);
  }

  void GetReadLock(const storage::TileGroupHeader *tile_group_header,
                   const oid_t tuple_id, txn_id_t txn_id) {
    txn_id_t *lock_addr = (txn_id_t *)(
        tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);

    while (true) {
      while (*lock_addr != INITIAL_TXN_ID)
        ;
      if (atomic_cas(lock_addr, INITIAL_TXN_ID, txn_id)) {
        return true;
      }
    }
  }

  void ReleaseReadLock(const storage::TileGroupHeader *tile_group_header,
                       const oid_t tuple_id, txn_id_t txn_id) {
    txn_id_t *lock_addr = (txn_id_t *)(
        tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);

    atomic_cas(lock_addr, txn_id, INITIAL_TXN_ID);
    //auto res = atomic_cas(lock_addr, txn_id, INITIAL_TXN_ID);
    //assert(res);
  }

  // Add the current txn into the reader list of a tuple
  void AddSIReader(storage::TileGroup *tile_group, const oid_t &tuple_id) {
    LOG_INFO("Add SSI reader");
    auto txn_id = current_txn->GetTransactionId();
    ReadList *reader = new ReadList(txn_id);
    reader->txn_id = txn_id;
    ReadList **headp = (ReadList **)(
        tile_group->GetHeader()->GetReservedFieldRef(tuple_id) + LIST_OFFSET);
    GetReadLock(tile_group->GetHeader(), tuple_id, txn_id);

    reader->next = *headp;
    *headp = reader;

    ReleaseReadLock(tile_group->GetHeader(), tuple_id, txn_id);
  }

  // Remove reader from the reader list of a tuple
  void RemoveSIReader(storage::TileGroup *tile_group, const oid_t tuple_id,
                      txn_id_t txn_id) {
    ReadList **headp = (ReadList **)(
        tile_group->GetHeader()->GetReservedFieldRef(tuple_id) + LIST_OFFSET);

    GetReadLock(tile_group->GetHeader(), tuple_id, txn_id);

    ReadList fake_header;
    fake_header.next = *headp;
    auto prev = &fake_header;
    auto next = prev->next;
    bool find = false;

    while (next != nullptr) {
      if (next->txn_id == txn_id) {
        find = true;
        prev->next = next->next;
        LOG_INFO("find in %ld group %ld tuple %ld", next->txn_id,
                 tile_group->GetTileGroupId(), tuple_id);
        delete next;
        break;
      }
      prev = next;
      next = next->next;
    }

    *headp = fake_header.next;

    ReleaseReadLock(tile_group->GetHeader(), tuple_id, txn_id);
    if (find == false) {
      assert(false);
    }
  }

  ReadList *GetReaderList(const storage::TileGroupHeader *tile_group_header,
                          const oid_t &tuple_id) {
    return *(ReadList **)(tile_group_header->GetReservedFieldRef(tuple_id) +
                          LIST_OFFSET);
  }

  bool GetInConflict(txn_id_t txn_id) {
    assert(txn_table_.count(txn_id) != 0);
    return txn_table_.at(txn_id).in_conflict_;
  }

  bool GetOutConflict(txn_id_t txn_id) {
    assert(txn_table_.count(txn_id) != 0);

    return txn_table_.at(txn_id).out_conflict_;
  }

  void SetInConflict(txn_id_t txn_id) {
    assert(txn_table_.count(txn_id) != 0);

    LOG_INFO("Set in conflict %lu", txn_id);
    txn_table_.at(txn_id).in_conflict_ = true;
  }

  void SetOutConflict(txn_id_t txn_id) {
    assert(txn_table_.count(txn_id) != 0);
    
    LOG_INFO("Set out conflict %lu", txn_id);
    txn_table_.at(txn_id).out_conflict_ = true;
  }

  void RemoveReader(txn_id_t txn_id);

  // Free contexts for SSI manager
  void CleanUp();

  static const int CREATOR_OFFSET = 0;
  static const int LOCK_OFFSET = (CREATOR_OFFSET + sizeof(txn_id_t));
  static const int LIST_OFFSET = (LOCK_OFFSET + sizeof(txn_id_t));
};
}
}
