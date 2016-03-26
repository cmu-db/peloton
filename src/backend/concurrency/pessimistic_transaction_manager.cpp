// //===----------------------------------------------------------------------===//
// //
// //                         PelotonDB
// //
// // transaction_manager.cpp
// //
// // Identification: src/backend/concurrency/pessimistic_transaction_manager.cpp
// //
// // Copyright (c) 2015, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

#include "pessimistic_transaction_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace concurrency {
PessimisticTransactionManager &PessimisticTransactionManager::GetInstance() {
  static PessimisticTransactionManager txn_manager;
  return txn_manager;
}

// // Visibility check
bool PessimisticTransactionManager::IsVisible(const txn_id_t &tuple_txn_id,
                                              const cid_t &tuple_begin_cid,
                                              const cid_t &tuple_end_cid) {

  if (EXTRACT_TXNID(tuple_txn_id) == EXTRACT_TXNID(INVALID_TXN_ID)) {
    // the tuple is not available.
    return false;
  }
  bool own = (EXTRACT_TXNID(current_txn->GetTransactionId()) == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    bool activated = (current_txn->GetStartCommitId() >= tuple_begin_cid);
    bool invalidated = (current_txn->GetStartCommitId() >= tuple_end_cid);
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // currently, we do not handle cascading abort. so never read an
        // uncommitted version.
        return false;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }
}

void PessimisticTransactionManager::ReleaseReadLock(__attribute__((unused)) storage::TileGroupHeader *tile_group_header, __attribute__((unused)) const oid_t &tuple_id){
}

bool PessimisticTransactionManager::AcquireTuple(storage::TileGroup *tile_group, const oid_t &tuple_id) {
  // acquire write lock.
  auto tile_group_header = tile_group->GetHeader();

  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  // No writer
  // Decrease read count
  if (EXTRACT_TXNID(old_txn_id) == INITIAL_TXN_ID) {
    while (true) {
      auto new_read_count = EXTRACT_READ_COUNT(old_txn_id) - 1;
      auto new_txn_id = PACK_TXNID(INITIAL_TXN_ID, new_read_count);
      auto res = tile_group_header->CASTxnId(tuple_id, new_txn_id, old_txn_id, &old_txn_id);
      if (!res) {
        // Assert there's no other writer
        assert(EXTRACT_TXNID(old_txn_id) == INITIAL_TXN_ID);
      } else {
        break;
      }
    }
  } else {
    // Has other writer
    return false;
  }

  // Try get write lock
  auto new_txn_id = current_txn->GetTransactionId();
  auto res = tile_group_header->CASTxnId(tuple_id, new_txn_id, INITIAL_TXN_ID, &old_txn_id);

  if (res) {
    return true;
  } else {
    LOG_INFO("Fail to insert new tuple. Set txn failure.");
    // TODO: Don't set txn result here
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
}

bool PessimisticTransactionManager::IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group->GetHeader()->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// No others own the tuple
bool PessimisticTransactionManager::IsAccessable(storage::TileGroup *tile_group,
                          const oid_t &tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool PessimisticTransactionManager::PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id) {
  // acquire read lock.
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  auto old_txn_id = tile_group_header->GetTransactionId(tuple_id);

  // No one is holding the write lock
  if (EXTRACT_TXNID(old_txn_id) == INITIAL_TXN_ID) {
    while (true) {
      auto new_read_count = EXTRACT_READ_COUNT(old_txn_id) + 1;
      // Try add read count
      auto new_txn_id = PACK_TXNID(INITIAL_TXN_ID, new_read_count);
      auto res = tile_group_header->CASTxnId(tuple_id, new_txn_id, old_txn_id, &old_txn_id);
      if (!res) {
        // See if there's writer
        if (EXTRACT_TXNID(old_txn_id) != INITIAL_TXN_ID)
          return false;
      } else {
        break;
      }
    }
  } else {
    // Has writer
    return false;
  }

  current_txn->RecordRead(tile_group_id, tuple_id);
  return true;
}

bool PessimisticTransactionManager::PerformWrite(const oid_t &tile_group_id, const oid_t &tuple_id, const ItemPointer &new_location) {
  // acquire write lock.
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();
  bool res = AcquireTuple(tile_group.get(), tuple_id);

  if (res) {
    SetUpdateVisibility(new_location.block, new_location.offset);
    tile_group_header->SetNextItemPointer(tuple_id, new_location);
    current_txn->RecordWrite(tile_group_id, tuple_id);
    return true;
  } else {
    // AcquireTuple may have changed the txn's result
    return false;
  }
}

bool PessimisticTransactionManager::PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id) {
  SetInsertVisibility(tile_group_id, tuple_id);
  current_txn->RecordInsert(tile_group_id, tuple_id);
  return true;
}

bool PessimisticTransactionManager::PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id, const ItemPointer &new_location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();
  auto res = AcquireTuple(tile_group.get(), tuple_id);

  if (res) {
    SetDeleteVisibility(new_location.block, new_location.offset);
    tile_group_header->SetNextItemPointer(tuple_id, new_location);
    current_txn->RecordDelete(tile_group_id, tuple_id);
    return true;
  } else {
    return false;
  }
}

Result PessimisticTransactionManager::CommitTransaction() {

  delete current_txn; 
  current_txn = nullptr;
  return Result::RESULT_ABORTED;
}

Result PessimisticTransactionManager::AbortTransaction() { 
  delete current_txn; 
  current_txn = nullptr;
  return Result::RESULT_ABORTED;
}

void PessimisticTransactionManager::SetDeleteVisibility(__attribute__((unused)) const oid_t &tile_group_id, __attribute__((unused))  const oid_t &tuple_id) {
    ;
}

void PessimisticTransactionManager::SetUpdateVisibility(__attribute__((unused))  const oid_t &tile_group_id, __attribute__((unused))  const oid_t &tuple_id) {

}

void PessimisticTransactionManager::SetInsertVisibility(__attribute__((unused))  const oid_t &tile_group_id, __attribute__((unused))  const oid_t &tuple_id) {

}

}
}