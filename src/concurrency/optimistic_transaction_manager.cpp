// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // optimistic_transaction_manager.cpp
// //
// // Identification: src/concurrency/optimistic_transaction_manager.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "concurrency/optimistic_transaction_manager.h"

// #include "catalog/manager.h"
// #include "common/exception.h"
// #include "common/logger.h"
// #include "common/platform.h"
// #include "concurrency/transaction.h"
// #include "gc/gc_manager_factory.h"
// #include "logging/log_manager.h"
// #include "logging/records/transaction_record.h"

// namespace peloton {
// namespace concurrency {

// OptimisticTransactionManager &
// OptimisticTransactionManager::GetInstance() {
//   static OptimisticTransactionManager txn_manager;
//   return txn_manager;
// }

// // check whether the current transaction owns the tuple version.
// // this function is called by update/delete executors.
// bool OptimisticTransactionManager::IsOwner(
//     Transaction *const current_txn,
//     const storage::TileGroupHeader *const tile_group_header,
//     const oid_t &tuple_id) {
//   auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

//   return tuple_txn_id == current_txn->GetTransactionId();
// }

// // This method tests whether the current transaction has created this version of
// // the tuple
// bool OptimisticTransactionManager::IsWritten(
//     Transaction *const current_txn,
//     const storage::TileGroupHeader *const tile_group_header,
//     const oid_t &tuple_id) {
//   return IsOwner(current_txn, tile_group_header, tuple_id) &&
//          tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID;
// }

// // if the tuple is not owned by any transaction and is visible to current
// // transaction.
// // this function is called by update/delete executors.
// bool OptimisticTransactionManager::IsOwnable(
//     Transaction *const current_txn,
//     const storage::TileGroupHeader *const tile_group_header,
//     const oid_t &tuple_id) {
//   auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
//   auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
//   return tuple_txn_id == INITIAL_TXN_ID && 
//          tuple_end_cid == MAX_CID;
// }

// // get write lock on a tuple.
// // this is invoked by update/delete executors.
// bool OptimisticTransactionManager::AcquireOwnership(
//     Transaction *const current_txn,
//     const storage::TileGroupHeader *const tile_group_header,
//     const oid_t &tuple_id) {
//   auto txn_id = current_txn->GetTransactionId();

//   if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
//     SetTransactionResult(Result::RESULT_FAILURE);
//     return false;
//   }
//   return true;
// }


// // release write lock on a tuple.
// // one example usage of this method is when a tuple is acquired, but operation
// // (insert,update,delete) can't proceed, the executor needs to yield the 
// // ownership before return false to upper layer.
// // It should not be called if the tuple is in the write set as commit and abort
// // will release the write lock anyway.
// void OptimisticTransactionManager::YieldOwnership(
//     UNUSED_ATTRIBUTE Transaction *const current_txn,
//     const oid_t &tile_group_id,
//     const oid_t &tuple_id) {
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
//   PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id));
//   tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
// }

// bool OptimisticTransactionManager::PerformRead(
//   Transaction *const current_txn, const ItemPointer &location,
//   bool acquire_ownership) {
//   if (current_txn->IsDeclaredReadOnly() == true) {
//     // Ignore read validation for all read-only transactions
//     return true;
//   }

//   oid_t tile_group_id = location.block;
//   oid_t tuple_id = location.offset;

//   LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group = manager.GetTileGroup(tile_group_id);
//   auto tile_group_header = tile_group->GetHeader();

//   // if it is select for update.
//   if (acquire_ownership == true) {
//     // if it is not owner, then we must try to acquire the ownership
//     if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
//       // Acquire ownership if we haven't
//       if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
//         // Cannot own
//         return false;
//       }
//       if (AcquireOwnership(current_txn, tile_group_header, tuple_id) == false) {
//         // Cannot acquire ownership
//         return false;
//       }
//       // Promote to RWType::READ_OWN
//       current_txn->RecordReadOwn(location);
//     }
//   } else {
//     current_txn->RecordRead(location);
//   }
//   return true;
// }

// void OptimisticTransactionManager::PerformInsert(
//   Transaction *const current_txn, const ItemPointer &location, 
//   ItemPointer *index_entry_ptr) {
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

//   oid_t tile_group_id = location.block;
//   oid_t tuple_id = location.offset;

//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
//   auto transaction_id = current_txn->GetTransactionId();

//   // Set MVCC info
//   // the tuple slot must be empty.
//   PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
//   PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
//   PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

//   tile_group_header->SetTransactionId(tuple_id, transaction_id);

//   // no need to set next item pointer.

//   // Add the new tuple into the insert set
//   current_txn->RecordInsert(location);

//   // Write down the head pointer's address in tile group header
//   tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

//   return true;
// }

// // remember to guarantee that at any time point,
// // a certain version (either old or new) of a tuple must be visible.
// // this function is invoked when it is the first time to update the tuple.
// // the tuple passed into this function is the global version.
// void OptimisticTransactionManager::PerformUpdate(
//     Transaction *const current_txn, const ItemPointer &old_location,
//     const ItemPointer &new_location) {
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);
//   auto transaction_id = current_txn->GetTransactionId();

//   auto tile_group_header = catalog::Manager::GetInstance()
//       .GetTileGroup(old_location.block)->GetHeader();
//   auto new_tile_group_header = catalog::Manager::GetInstance()
//       .GetTileGroup(new_location.block)->GetHeader();

//   // if we can perform update, then we must have already locked the older
//   // version.
//   PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
//          transaction_id);
//   PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
//          INVALID_TXN_ID);
//   PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
//          MAX_CID);
//   PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

//   // Set double linked list in a newest to oldest manner
//   new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

//   tile_group_header->SetPrevItemPointer(old_location.offset, new_location);
  
//   new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

//   // Set the header information for the new version
//   ItemPointer *index_entry_ptr = tile_group_header->GetIndirection(old_location.offset);
  
//   if (index_entry_ptr != nullptr) {

//     new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);

//     // Set the index header in an atomic way.
//     // We do it atomically because we don't want any one to see a half-done pointer.
//     // In case of contention, no one can update this pointer when we are updating it
//     // because we are holding the write lock. This update should success in its first trial.
//     UNUSED_ATTRIBUTE res = AtomicUpdateItemPointer(index_entry_ptr, new_location);
//     PL_ASSERT(res == true);

//     // Add the old tuple into the update set
//     current_txn->RecordUpdate(old_location); 
//   }
// }

// // this function is invoked when it is NOT the first time to update the tuple.
// // the tuple passed into this function is the local version created by this txn.
// void OptimisticTransactionManager::PerformUpdate(
//     Transaction *const current_txn, const ItemPointer &location) {
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

//   oid_t tile_group_id = location.block;
//   oid_t tuple_id = location.offset;

//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

//   PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
//          current_txn->GetTransactionId());
//   PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
//   PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

//   // Add the old tuple into the update set
//   auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
//   if (old_location.IsNull() == false) {
//     // if this version is not newly inserted.
//     current_txn->RecordUpdate(old_location);
//   }
// }

// void OptimisticTransactionManager::PerformDelete(
//     Transaction *const current_txn, const ItemPointer &old_location,
//     const ItemPointer &new_location) {
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);
//   auto transaction_id = current_txn->GetTransactionId();

//   auto tile_group_header = catalog::Manager::GetInstance()
//       .GetTileGroup(old_location.block)->GetHeader();
//   auto new_tile_group_header = catalog::Manager::GetInstance()
//       .GetTileGroup(new_location.block)->GetHeader();

//   // if we can perform update, then we must have already locked the older
//   // version.
//   PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
//          transaction_id);
//   PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
//          INVALID_TXN_ID);
//   PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
//          MAX_CID);
//   PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) == 
//          MAX_CID);

//   // Set the timestamp for delete
//   new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  
//   new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

//   // Set double linked list in a newest to oldest manner
//   new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);
  
//   tile_group_header->SetPrevItemPointer(old_location.offset, new_location);
  
//   new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

//   // Set the header information for the new version
//   ItemPointer *index_entry_ptr = tile_group_header->GetIndirection(old_location.offset);

//   if (index_entry_ptr != nullptr) {

//     new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);

//     // Set the index header in an atomic way.
//     // We do it atomically because we don't want any one to see a half-down pointer
//     // In case of contention, no one can update this pointer when we are updating it
//     // because we are holding the write lock. This update should success in its first trial.
//     UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(index_entry_ptr, new_location);
//     PL_ASSERT(res == true);
    
//     // Add the old tuple into the delete set
//     current_txn->RecordDelete(old_location);
//   }
// }

// void OptimisticTransactionManager::PerformDelete(
//     Transaction *const current_txn, const ItemPointer &location) {
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);
  
//   oid_t tile_group_id = location.block;
//   oid_t tuple_id = location.offset;

//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

//   PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
//          current_txn->GetTransactionId());
//   PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

//   tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

//   // Add the old tuple into the delete set
//   auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
//   if (old_location.IsNull() == false) {
//     // if this version is not newly inserted.
//     current_txn->RecordDelete(old_location);
//   } else {
//     // if this version is newly inserted.
//     current_txn->RecordDelete(location);
//   }
// }

// ResultType OptimisticTransactionManager::CommitTransaction(
//     Transaction *const current_txn) {
//   LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

//   auto &manager = catalog::Manager::GetInstance();

//   auto &rw_set = current_txn->GetReadWriteSet();

//   //*****************************************************
//   // we can optimize read-only transaction.
//   if (current_txn->IsReadOnly() == true) {
//     // validate read set.
//     for (auto &tile_group_entry : rw_set) {
//       oid_t tile_group_id = tile_group_entry.first;
//       auto tile_group = manager.GetTileGroup(tile_group_id);
//       auto tile_group_header = tile_group->GetHeader();
//       for (auto &tuple_entry : tile_group_entry.second) {
//         auto tuple_slot = tuple_entry.first;
//         // if this tuple is not newly inserted.
//         if (tuple_entry.second == RW_TYPE_READ) {
//           if (tile_group_header->GetTransactionId(tuple_slot) ==
//                   INITIAL_TXN_ID &&
//               tile_group_header->GetBeginCommitId(tuple_slot) <=
//                   current_txn->GetBeginCommitId() &&
//               tile_group_header->GetEndCommitId(tuple_slot) >=
//                   current_txn->GetBeginCommitId()) {
//             // the version is not owned by other txns and is still visible.
//             continue;
//           }
//           // otherwise, validation fails. abort transaction.
//           AddOneReadAbort();
//           return AbortTransaction();
//         } else {
//           PL_ASSERT(tuple_entry.second == RW_TYPE_INS_DEL);
//         }
//       }
//     }
//     // is it always true???
//     Result ret = current_txn->GetResult();
//     gc::GCManagerFactory::GetInstance().EndGCContext(INVALID_CID);
//     EndTransaction();
//     return ret;
//   }
//   //*****************************************************

//   // generate transaction id.
//   cid_t end_commit_id = GetNextCommitId();

//   // validate read set.
//   for (auto &tile_group_entry : rw_set) {
//     oid_t tile_group_id = tile_group_entry.first;
//     auto tile_group = manager.GetTileGroup(tile_group_id);
//     auto tile_group_header = tile_group->GetHeader();
//     for (auto &tuple_entry : tile_group_entry.second) {
//       auto tuple_slot = tuple_entry.first;
//       // if this tuple is not newly inserted.
//       if (tuple_entry.second != RW_TYPE_INSERT &&
//           tuple_entry.second != RW_TYPE_INS_DEL) {
//         // if this tuple is owned by this txn, then it is safe.
//         if (tile_group_header->GetTransactionId(tuple_slot) ==
//             current_txn->GetTransactionId()) {
//           // the version is owned by the transaction.
//           continue;
//         } else {
//           if (tile_group_header->GetTransactionId(tuple_slot) ==
//                   INITIAL_TXN_ID && tile_group_header->GetBeginCommitId(
//                                         tuple_slot) <= end_commit_id &&
//               tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
//             // the version is not owned by other txns and is still visible.
//             continue;
//           }
//         }
//         LOG_TRACE("transaction id=%lu",
//                   tile_group_header->GetTransactionId(tuple_slot));
//         LOG_TRACE("begin commit id=%lu",
//                   tile_group_header->GetBeginCommitId(tuple_slot));
//         LOG_TRACE("end commit id=%lu",
//                   tile_group_header->GetEndCommitId(tuple_slot));
//         // otherwise, validation fails. abort transaction.
//         AddOneReadAbort();
//         return AbortTransaction();
//       }
//     }
//   }
//   //////////////////////////////////////////////////////////

//   // auto &log_manager = logging::LogManager::GetInstance();
//   // log_manager.LogBeginTransaction(end_commit_id);
//   // install everything.
//   for (auto &tile_group_entry : rw_set) {
//     oid_t tile_group_id = tile_group_entry.first;
//     auto tile_group = manager.GetTileGroup(tile_group_id);
//     auto tile_group_header = tile_group->GetHeader();
//     for (auto &tuple_entry : tile_group_entry.second) {
//       auto tuple_slot = tuple_entry.first;
//       if (tuple_entry.second == RW_TYPE_UPDATE) {
//         // logging.
//         ItemPointer new_version =
//             tile_group_header->GetPrevItemPointer(tuple_slot);
//         //ItemPointer old_version(tile_group_id, tuple_slot);

//         // logging.
//         // log_manager.LogUpdate(current_txn, end_commit_id, old_version,
//         //                      new_version);

//         // we must guarantee that, at any time point, AT LEAST ONE version is
//         // visible.
//         // we do not change begin cid for old tuple.
//         auto new_tile_group_header =
//             manager.GetTileGroup(new_version.block)->GetHeader();

//         new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
//         new_tile_group_header->SetBeginCommitId(new_version.offset,
//                                                 end_commit_id);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

//         COMPILER_MEMORY_FENCE;

//         new_tile_group_header->SetTransactionId(new_version.offset,
//                                                 INITIAL_TXN_ID);
//         tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//         // GC recycle.
//         RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

//       } else if (tuple_entry.second == RW_TYPE_DELETE) {
//         ItemPointer new_version =
//             tile_group_header->GetPrevItemPointer(tuple_slot);
//         ItemPointer delete_location(tile_group_id, tuple_slot);

//         // logging.
//         // log_manager.LogDelete(end_commit_id, delete_location);

//         // we do not change begin cid for old tuple.
//         auto new_tile_group_header =
//             manager.GetTileGroup(new_version.block)->GetHeader();

//         new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
//         new_tile_group_header->SetBeginCommitId(new_version.offset,
//                                                 end_commit_id);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

//         COMPILER_MEMORY_FENCE;

//         new_tile_group_header->SetTransactionId(new_version.offset,
//                                                 INVALID_TXN_ID);
//         tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//         // GC recycle.
//         RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

//       } else if (tuple_entry.second == RW_TYPE_INSERT) {
//         PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
//                current_txn->GetTransactionId());
//         // set the begin commit id to persist insert
//         ItemPointer insert_location(tile_group_id, tuple_slot);
//         // log_manager.LogInsert(current_txn, end_commit_id, insert_location);

//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
//         tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//       } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
//         PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
//                current_txn->GetTransactionId());

//         // set the begin commit id to persist insert
//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
//         tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
//       }
//     }
//   }
//   // log_manager.LogCommitTransaction(end_commit_id);

//   gc::GCManagerFactory::GetInstance().EndGCContext(end_commit_id);
//   EndTransaction();

//   return Result::RESULT_SUCCESS;
// }

// Result OptimisticTransactionManager::AbortTransaction(
//     Transaction *const current_txn) {
//   // It's impossible that a pre-declared read-only transaction aborts
//   PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

//   LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
//   auto &manager = catalog::Manager::GetInstance();

//   auto &rw_set = current_txn->GetReadWriteSet();
  
//   std::vector<ItemPointer> aborted_versions;

//   for (auto &tile_group_entry : rw_set) {
//     oid_t tile_group_id = tile_group_entry.first;
//     auto tile_group = manager.GetTileGroup(tile_group_id);
//     auto tile_group_header = tile_group->GetHeader();

//     for (auto &tuple_entry : tile_group_entry.second) {
//       auto tuple_slot = tuple_entry.first;
//       if (tuple_entry.second == RW_TYPE_UPDATE) {
//         // we do not set begin cid for old tuple.
//         ItemPointer new_version =
//             tile_group_header->GetPrevItemPointer(tuple_slot);

//         auto new_tile_group_header =
//             manager.GetTileGroup(new_version.block)->GetHeader();
//         new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
//         new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

//         COMPILER_MEMORY_FENCE;

//         // TODO: actually, there's no need to reset the end commit id.
//         PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

//         // We must first adjust the head pointer
//         // before we unlink the aborted version from version list
//         auto head_ptr = GetHeadPtr(tile_group_header, tuple_slot);
//         auto res = AtomicUpdateItemPointer(head_ptr, ItemPointer(tile_group_id, tuple_slot));
//         PL_ASSERT(res == true);
//         (void) res;

//         COMPILER_MEMORY_FENCE;

//         new_tile_group_header->SetTransactionId(new_version.offset,
//                                                 INVALID_TXN_ID);

//         tile_group_header->SetPrevItemPointer(tuple_slot, INVALID_ITEMPOINTER);

//         // NOTE: We cannot do the unlink here because maybe someone is still traversing
//         // the aborted version. Such unlink will isolate such travelers. The unlink task
//         // should be offload to GC
//         // new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//         aborted_versions.push_back(new_version);
//         // GC recycle
//         //RecycleOldTupleSlot(new_version.block, new_version.offset, current_txn->GetBeginCommitId());

//       } else if (tuple_entry.second == RW_TYPE_DELETE) {
//         ItemPointer new_version =
//             tile_group_header->GetPrevItemPointer(tuple_slot);

//         auto new_tile_group_header =
//             manager.GetTileGroup(new_version.block)->GetHeader();

//         new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
//         new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

//         // We must fisrt adjust the head pointer
//         // before we unlink the aborted version from version list
//         auto head_ptr = GetHeadPtr(tile_group_header, tuple_slot);
//         auto res = AtomicUpdateItemPointer(head_ptr, ItemPointer(tile_group_id, tuple_slot));
//         PL_ASSERT(res == true);
//         (void) res;

//         COMPILER_MEMORY_FENCE;

//         new_tile_group_header->SetTransactionId(new_version.offset,
//                                                 INVALID_TXN_ID);

//         // reset the item pointers.
//         tile_group_header->SetPrevItemPointer(tuple_slot, INVALID_ITEMPOINTER);


//         // NOTE: We cannot do the unlink here because maybe someone is still traversing
//         // the aborted version. Such unlink will isolate such travelers. The unlink task
//         // should be offload to GC
//         //new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//         aborted_versions.push_back(new_version);
//         // GC recycle
//         //RecycleOldTupleSlot(new_version.block, new_version.offset, current_txn->GetBeginCommitId());

//       } else if (tuple_entry.second == RW_TYPE_INSERT) {
//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
//         tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

//         // aborted_versions.push_back(ItemPointer(tile_group_id, tuple_slot));
//         // GC recycle
//         //RecycleInvalidTupleSlot(tile_group_id, tuple_slot);

//       } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
//         tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
//         tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

//         COMPILER_MEMORY_FENCE;

//         tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

//         // aborted_versions.push_back(ItemPointer(tile_group_id, tuple_slot));
//         // GC recycle
//         //RecycleInvalidTupleSlot(tile_group_id, tuple_slot);
        
//       }
//     }
//   }

//   cid_t next_commit_id = GetNextCommitId();

//   for (auto &item_pointer : aborted_versions) {
//     RecycleOldTupleSlot(item_pointer.block, item_pointer.offset, next_commit_id);
//   }

//   // Need to change next_commit_id to INVALID_CID if disable the recycle of aborted version
//   gc::GCManagerFactory::GetInstance().EndGCContext(next_commit_id);
//   EndTransaction();
//   return Result::RESULT_ABORTED;
// }

// }  // End storage namespace
// }  // End peloton namespace
