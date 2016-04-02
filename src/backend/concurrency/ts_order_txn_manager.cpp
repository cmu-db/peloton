// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // pessimistic_txn_manager.cpp
// //
// // Identification: src/backend/concurrency/ts_order_txn_manager.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "ts_order_txn_manager.h"

// #include "backend/common/platform.h"
// #include "backend/logging/log_manager.h"
// #include "backend/logging/records/transaction_record.h"
// #include "backend/concurrency/transaction.h"
// #include "backend/catalog/manager.h"
// #include "backend/common/exception.h"
// #include "backend/common/logger.h"
// #include "backend/storage/data_table.h"
// #include "backend/storage/tile_group.h"
// #include "backend/storage/tile_group_header.h"

// namespace peloton {
// namespace concurrency {

// TsOrderTxnManager &TsOrderTxnManager::GetInstance() {
//   static TsOrderTxnManager txn_manager;
//   return txn_manager;
// }

// // Visibility check
// bool TsOrderTxnManager::IsVisible(const storage::TileGroupHeader * const tile_group_header,
//                                              const oid_t &tuple_id) {
//   txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
//   cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
//   cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

//   if (tuple_txn_id == INVALID_TXN_ID) {
//     // the tuple is not available.
//     return false;
//   }
//   bool own = (current_txn->GetTransactionId() == tuple_txn_id);

//   // there are exactly two versions that can be owned by a transaction.
//   // unless it is an insertion.
//   if (own == true) {
//     if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
//       assert(tuple_end_cid == MAX_CID);
//       // the only version that is visible is the newly inserted one.
//       return true;
//     } else {
//       // the older version is not visible.
//       return false;
//     }
//   } else {
//     bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
//     bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
//     if (tuple_txn_id != INITIAL_TXN_ID) {
//       // if the tuple is owned by other transactions.
//       if (tuple_begin_cid == MAX_CID) {
//         // currently, we do not handle cascading abort. so never read an
//         // uncommitted version.
//         return false;
//       } else {
//         // the older version may be visible.
//         if (activated && !invalidated) {
//           return true;
//         } else {
//           return false;
//         }
//       }
//     } else {
//       // if the tuple is not owned by any transaction.
//       if (activated && !invalidated) {
//         return true;
//       } else {
//         return false;
//       }
//     }
//   }
// }

// bool TsOrderTxnManager::IsOwner(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id) {  
//   auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
//   return tuple_txn_id == current_txn->GetTransactionId();
// }

// bool TsOrderTxnManager::IsOwnable(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id) {
//   auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
//   auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
//   return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
// }

// bool TsOrderTxnManager::AcquireOwnership(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id) {
//   auto txn_id = current_txn->GetTransactionId();

//   if (tile_group_header->LockTupleSlot(tuple_id, txn_id) == false) {
//     LOG_INFO("Fail to insert new tuple. Set txn failure.");
//     SetTransactionResult(Result::RESULT_FAILURE);
//     return false;
//   }
//   // change timestamp 
//   return true;
// }

// void TsOrderTxnManager::SetInsertVisibility(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
//   auto transaction_id = current_txn->GetTransactionId();

//   // Set MVCC info
//   assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
//   assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
//   assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

//   tile_group_header->SetTransactionId(tuple_id, transaction_id);
//   tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
//   tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

//   // tile_group_header->SetInsertCommit(tuple_id, false); // unused
//   // tile_group_header->SetDeleteCommit(tuple_id, false); // unused
// }

// bool TsOrderTxnManager::PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   SetInsertVisibility(tile_group_id, tuple_id);
//   current_txn->RecordInsert(tile_group_id, tuple_id);
//   return true;
// }

// bool TsOrderTxnManager::PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id) {
//   LOG_TRACE("Perform read");
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group = manager.GetTileGroup(tile_group_id);
//   auto tile_group_header = tile_group->GetHeader();
//   // if the version we want to read is uncommitted, then abort.

//   current_txn->RecordRead(tile_group_id, tuple_id);
//   return true;
// }

// bool TsOrderTxnManager::PerformUpdate(
//     const oid_t &tile_group_id, const oid_t &tuple_id,
//     const ItemPointer &new_location) {
//   LOG_INFO("Performing Write %lu %lu", tile_group_id, tuple_id);

//   auto &manager = catalog::Manager::GetInstance();
//   auto transaction_id = current_txn->GetTransactionId();
//   auto tile_group = manager.GetTileGroup(tile_group_id);
//   auto tile_group_header = tile_group->GetHeader();
//   auto new_tile_group_header = catalog::Manager::GetInstance()
//     .GetTileGroup(new_location.block)
//     ->GetHeader();

//   // The write lock must have been acquired
//   // Notice: if the executor doesn't call PerformUpdate after AcquireOwnership, no
//   // one will possibly release the write lock acquired by this txn.
//   // Set double linked list
//   tile_group_header->SetNextItemPointer(tuple_id, new_location);
//   new_tile_group_header->SetPrevItemPointer(new_location.offset, ItemPointer(tile_group_id, tuple_id));

//   new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
//   new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
//   new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);
//   PerformUpdate(new_location.block, new_location.offset);

//   // Add the old tuple into the update set
//   current_txn->RecordUpdate(tile_group_id, tuple_id);
//   return true;
// }

// bool TsOrderTxnManager::PerformDelete(
//     const oid_t &tile_group_id, const oid_t &tuple_id,
//     const ItemPointer &new_location) {
//   LOG_TRACE("Performing Delete");
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group = manager.GetTileGroup(tile_group_id);
//   auto tile_group_header = tile_group->GetHeader();
//   auto res = AcquireOwnership(tile_group_header, tile_group_id, tuple_id);

//   if (res) {
//     auto new_tile_group_header = catalog::Manager::GetInstance()
//       .GetTileGroup(new_location.block)
//       ->GetHeader();
//     auto transaction_id = current_txn->GetTransactionId();

//     // Set up double linked list
//     tile_group_header->SetNextItemPointer(tuple_id, new_location);
//     new_tile_group_header->SetPrevItemPointer(new_location.offset, ItemPointer(tile_group_id, tuple_id));

//     new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
//     new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
//     new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

//     current_txn->RecordDelete(tile_group_id, tuple_id);
//     return true;
//   } else {
//     return false;
//   }
// }

// void TsOrderTxnManager::PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id) {
// }

// void TsOrderTxnManager::PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id) {
// }

// Result TsOrderTxnManager::CommitTransaction() {
//   Result ret = current_txn->GetResult();

//   EndTransaction();

//   return ret;
// }

// Result TsOrderTxnManager::AbortTransaction() {

//   EndTransaction();
//   return Result::RESULT_ABORTED;
// }


// }
// }