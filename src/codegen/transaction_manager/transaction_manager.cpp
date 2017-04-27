//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/types.h"
#include "codegen/transaction_manager/transaction_manager.h"
#include "catalog/manager.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"

static peloton::storage::TileGroupHeader *pl_txn_get_tile_group_header_by_id(
    oid_t tile_group_id) {
  auto &manager = peloton::catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  return tile_group_header;
}

cid_t pl_txn_get_next_commit_id(TransactionManager *txn_mgr) {
  return txn_mgr->GetNextCommitId();
}

cid_t pl_txn_get_current_commit_id(TransactionManager *txn_mgr) {
  return txn_mgr->GetCurrentCommitId();
}

// bool pl_txn_is_occupied(TransactionManager *txn_mgr, Transaction *txn,
//                         oid_t tile_group_id, oid_t tuple_id);

int pl_txn_is_visible(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id) {
  auto tile_group_header = pl_txn_get_tile_group_header_by_id(tile_group_id);
  switch (txn_mgr->IsVisible(txn, tile_group_header, tuple_id)) {
    case peloton::VisibilityType::INVISIBLE:
      return VISIBILITY_INVISIBLE;

    case peloton::VisibilityType::DELETED:
      return VISIBILITY_DELETED;

    case peloton::VisibilityType::OK:
      return VISIBILITY_OK;

    default:
      PL_ASSERT("Invalid Visibility");
      return -1; // Shouldn't reach here.
  }
}

bool pl_txn_is_owner(TransactionManager *txn_mgr, Transaction *txn,
                     oid_t tile_group_id, oid_t tuple_id) {
  auto tile_group_header = pl_txn_get_tile_group_header_by_id(tile_group_id);
  return txn_mgr->IsOwner(txn, tile_group_header, tuple_id);
}

bool pl_txn_is_written(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id) {
  auto tile_group_header = pl_txn_get_tile_group_header_by_id(tile_group_id);
  return txn_mgr->IsWritten(txn, tile_group_header, tuple_id);
}

bool pl_txn_is_ownable(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id) {
  auto tile_group_header = pl_txn_get_tile_group_header_by_id(tile_group_id);
  return txn_mgr->IsOwnable(txn, tile_group_header, tuple_id);
}

bool pl_txn_acquire_ownership(TransactionManager *txn_mgr, Transaction *txn,
                              oid_t tile_group_id, oid_t tuple_id) {
  auto tile_group_header = pl_txn_get_tile_group_header_by_id(tile_group_id);
  return txn_mgr->AcquireOwnership(txn, tile_group_header, tuple_id);
}

void pl_txn_yield_ownership(TransactionManager *txn_mgr, Transaction *txn,
                            oid_t tile_group_id, oid_t tuple_id) {
  return txn_mgr->YieldOwnership(txn, tile_group_id, tuple_id);
}

void pl_txn_perform_insert(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t tile_group_id, oid_t tuple_id,
                           ItemPointer *index_entry_ptr) {
  ItemPointer item_pointer(tile_group_id, tuple_id);
  txn_mgr->PerformInsert(txn, item_pointer, index_entry_ptr);
}

void pl_txn_perform_update_move(TransactionManager *txn_mgr, Transaction *txn,
                                oid_t old_tile_group_id, oid_t old_tuple_id,
                                oid_t new_tile_group_id, oid_t new_tuple_id) {
  ItemPointer old_location(old_tile_group_id, old_tuple_id);
  ItemPointer new_location(new_tile_group_id, new_tuple_id);
  return txn_mgr->PerformUpdate(txn, old_location, new_location);
}

void pl_txn_perform_delete_move(TransactionManager *txn_mgr, Transaction *txn,
                                oid_t old_tile_group_id, oid_t old_tuple_id,
                                oid_t new_tile_group_id, oid_t new_tuple_id) {
  ItemPointer old_location(old_tile_group_id, old_tuple_id);
  ItemPointer new_location(new_tile_group_id, new_tuple_id);
  return txn_mgr->PerformDelete(txn, old_location, new_location);
}

void pl_txn_perform_update(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t old_tile_group_id, oid_t old_tuple_id) {
  ItemPointer old_location(old_tile_group_id, old_tuple_id);
  return txn_mgr->PerformUpdate(txn, old_location);
}

void pl_txn_perform_delete(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t old_tile_group_id, oid_t old_tuple_id) {
  ItemPointer old_location(old_tile_group_id, old_tuple_id);
  return txn_mgr->PerformDelete(txn, old_location);
}
