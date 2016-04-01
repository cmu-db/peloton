////===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// rowo_txn_manager.h
////
//// Identification: src/backend/concurrency/ssi_txn_manager.h
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//#pragma once
//
//#include "backend/concurrency/transaction_manager.h"
//#include "backend/storage/tile_group.h"
//
//namespace peloton {
//namespace concurrency {
//
//  struct SsiTxnContext {
//    Transaction *transaction_;
//    bool in_conflict_;
//    bool out_conflict_;
//  };
//
//class SsiTxnManager : public TransactionManager {
// public:
//  SsiTxnManager() {}
//
//  virtual ~SsiTxnManager() {}
//
//  static SsiTxnManager &GetInstance();
//
//  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
//                         const cid_t &tuple_begin_cid,
//                         const cid_t &tuple_end_cid);
//
//  virtual bool IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id);
//
//  virtual bool IsAccessable(storage::TileGroup *tile_group,
//                            const oid_t &tuple_id);
//
//  virtual bool AcquireLock(const storage::TileGroupHeader * const tile_group_header,
//                            const oid_t &tuple_id);
//
//  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);
//
//  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
//                             const ItemPointer &new_location);
//
//  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);
//
//  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
//                             const ItemPointer &new_location);
//
//  virtual void SetInsertVisibility(const oid_t &tile_group_id,
//                                   const oid_t &tuple_id);
//
//  virtual void PerformDelete(const oid_t &tile_group_id,
//                                   const oid_t &tuple_id);
//
//  virtual void PerformUpdate(const oid_t &tile_group_id,
//                                   const oid_t &tuple_id);
//
//  virtual Transaction *BeginTransaction() {
//    Transaction *txn = TransactionManager::BeginTransaction();
//    {
//      std::lock_guard<std::mutex> lock(running_txns_mutex_);
//      assert(running_txns_.find(txn->GetTransactionId()) == running_txns_.end());
//      running_txns_[txn->GetTransactionId()];
//    }
//    return txn;
//  }
//
//  virtual void EndTransaction() {}
//
//  virtual Result CommitTransaction();
//
//  virtual Result AbortTransaction();
//
//private:
//  std::mutex running_txns_mutex_;
//  std::map<txn_id_t, std::pair<SsiTxnContext> running_txns_;
//};
//}
//}
