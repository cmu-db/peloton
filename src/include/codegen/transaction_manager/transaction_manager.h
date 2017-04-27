//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/codegen/transaction_manager/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * This file contains the C API of the transaction manager.
 *
 * A C API has a cleaner interface - no name mangling, no virtual functions ...
 */

#pragma once

#include "type/types.h"
#include "concurrency/transaction_manager.h"

using peloton::cid_t;
using peloton::oid_t;
using peloton::concurrency::TransactionManager;
using peloton::concurrency::Transaction;
using peloton::ItemPointer;

extern "C" {

#define VISIBILITY_INVISIBLE 1
#define VISIBILITY_DELETED   2
#define VISIBILITY_OK        3

cid_t pl_txn_get_next_commit_id(TransactionManager *txn_mgr);

cid_t pl_txn_get_current_commit_id(TransactionManager *txn_mgr);

// bool pl_txn_is_occupied(TransactionManager *txn_mgr, Transaction *txn,
//                         oid_t tile_group_id, oid_t tuple_id);

int pl_txn_is_visible(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id);

bool pl_txn_is_owner(TransactionManager *txn_mgr, Transaction *txn,
                     oid_t tile_group_id, oid_t tuple_id);

bool pl_txn_is_written(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id);

bool pl_txn_is_ownable(TransactionManager *txn_mgr, Transaction *txn,
                       oid_t tile_group_id, oid_t tuple_id);

bool pl_txn_acquire_ownership(TransactionManager *txn_mgr, Transaction *txn,
                              oid_t tile_group_id, oid_t tuple_id);

void pl_txn_yield_ownership(TransactionManager *txn_mgr, Transaction *txn,
                            oid_t tile_group_id, oid_t tuple_id);

void pl_txn_perform_insert(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t tile_group_id, oid_t tuple_id,
                           ItemPointer *index_entry_ptr);

void pl_txn_perform_update_move(TransactionManager *txn_mgr, Transaction *txn,
                                oid_t old_tile_group_id, oid_t old_tuple_id,
                                oid_t new_tile_group_id, oid_t new_tuple_id);

void pl_txn_perform_delete_move(TransactionManager *txn_mgr, Transaction *txn,
                                oid_t old_tile_group_id, oid_t old_tuple_id,
                                oid_t new_tile_group_id, oid_t new_tuple_id);

void pl_txn_perform_update(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t old_tile_group_id, oid_t old_tuple_id);

void pl_txn_perform_delete(TransactionManager *txn_mgr, Transaction *txn,
                           oid_t old_tile_group_id, oid_t old_tuple_id,
                           oid_t new_tile_group_id, oid_t new_tuple_id);

}
