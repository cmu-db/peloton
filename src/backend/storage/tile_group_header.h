//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.h
//
// Identification: src/backend/storage/tile_group_header.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"
#include "backend/common/platform.h"
#include "backend/common/printable.h"
#include "backend/logging/log_manager.h"

#include <atomic>
#include <mutex>
#include <iostream>
#include <cassert>
#include <queue>
#include <cstring>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

/**
 *
 * This contains information related to MVCC.
 * It is shared by all tiles in a tile group.
 *
 * Layout :
 *
 *  -----------------------------------------------------------------------------
 *  | TxnID (8 bytes)  | BeginTimeStamp (8 bytes) | EndTimeStamp (8 bytes) |
 *  | NextItemPointer (16 bytes) | PrevItemPointer (16 bytes) | ReservedField (24 bytes)
 *  | InsertCommit (1 byte) | DeleteCommit (1 byte)
 *  -----------------------------------------------------------------------------
 *
 */

#define TUPLE_HEADER_LOCATION data + (tuple_slot_id * header_entry_size)

class TileGroupHeader : public Printable {
  TileGroupHeader() = delete;

 public:
  TileGroupHeader(const BackendType &backend_type, const int &tuple_count);

  TileGroupHeader &operator=(const peloton::storage::TileGroupHeader &other) {
    // check for self-assignment
    if (&other == this) return *this;

    header_size = other.header_size;

    // copy over all the data
    memcpy(data, other.data, header_size);

    num_tuple_slots = other.num_tuple_slots;
    unsigned long long val = other.next_tuple_slot;
    next_tuple_slot = val;

    return *this;
  }

  ~TileGroupHeader();

  // this function is only called by DataTable::GetEmptyTupleSlot().
  oid_t GetNextEmptyTupleSlot() {
    oid_t tuple_slot_id = next_tuple_slot.fetch_add(1, std::memory_order_relaxed);

    if (tuple_slot_id >= num_tuple_slots) {
      return INVALID_OID;
    } else {
      return tuple_slot_id;
    }
  }

  /**
   * Used by logging
   */
   // TODO: rewrite the code!!!
  bool GetEmptyTupleSlot(const oid_t &tuple_slot_id) {
    tile_header_lock.Lock();
    if (tuple_slot_id < num_tuple_slots) {
      if (next_tuple_slot <= tuple_slot_id) {
        next_tuple_slot = tuple_slot_id + 1;
      }
      tile_header_lock.Unlock();
      return true;
    } else {
      tile_header_lock.Unlock();
      return false;
    }
  }

  oid_t GetNextTupleSlot() const { return next_tuple_slot; }

  //oid_t GetActiveTupleCount(const txn_id_t &txn_id);

  oid_t GetActiveTupleCount();

  //===--------------------------------------------------------------------===//
  // MVCC utilities
  //===--------------------------------------------------------------------===//

  // Getters

  // DOUBLE CHECK: whether we need atomic load???
  // it is possible that some other transactions are modifying the txn_id,
  // but the current transaction reads the txn_id.
  // the returned value seems to be uncertain.
  inline txn_id_t GetTransactionId(const oid_t &tuple_slot_id) const {
    //txn_id_t *txn_id_ptr = (txn_id_t *)(TUPLE_HEADER_LOCATION);
    //return __atomic_load_n(txn_id_ptr, __ATOMIC_RELAXED);
    return *((txn_id_t *)(TUPLE_HEADER_LOCATION));
  }

  inline cid_t GetBeginCommitId(const oid_t &tuple_slot_id) const {
    return *((cid_t *)(TUPLE_HEADER_LOCATION + begin_cid_offset));
  }

  inline cid_t GetEndCommitId(const oid_t &tuple_slot_id) const {
    return *((cid_t *)(TUPLE_HEADER_LOCATION + end_cid_offset));
  }

  inline ItemPointer GetNextItemPointer(const oid_t &tuple_slot_id) const {
    return *((ItemPointer *)(TUPLE_HEADER_LOCATION + next_pointer_offset));
  }

  inline ItemPointer GetPrevItemPointer(const oid_t &tuple_slot_id) const {
    return *((ItemPointer *)(TUPLE_HEADER_LOCATION + prev_pointer_offset));
  }

  // constraint: at most 24 bytes.
  inline char* GetReservedFieldRef(const oid_t &tuple_slot_id) const {
    return (char *)(TUPLE_HEADER_LOCATION + reserved_field_offset);
  }

  inline bool GetInsertCommit(const oid_t &tuple_slot_id) const {
    return *((bool *)(TUPLE_HEADER_LOCATION + insert_commit_offset));
  }

  inline bool GetDeleteCommit(const oid_t &tuple_slot_id) const {
    return *((bool *)(TUPLE_HEADER_LOCATION + delete_commit_offset));
  }

  // Setters
  inline void SetTransactionId(const oid_t &tuple_slot_id,
                               const txn_id_t &transaction_id) {
    *((txn_id_t *)(TUPLE_HEADER_LOCATION)) = transaction_id;
  }

  inline void SetBeginCommitId(const oid_t &tuple_slot_id,
                               const cid_t &begin_cid) {
    *((cid_t *)(TUPLE_HEADER_LOCATION + begin_cid_offset)) = begin_cid;
  }

  inline void SetEndCommitId(const oid_t &tuple_slot_id,
                             const cid_t &end_cid) const {
    *((cid_t *)(TUPLE_HEADER_LOCATION + end_cid_offset)) = end_cid;
  }

  inline void SetNextItemPointer(const oid_t &tuple_slot_id,
                                 const ItemPointer &item) const {
    *((ItemPointer *)(TUPLE_HEADER_LOCATION + next_pointer_offset)) = item;
  }

  inline void SetPrevItemPointer(const oid_t &tuple_slot_id,
                                 const ItemPointer &item) const {
    *((ItemPointer *)(TUPLE_HEADER_LOCATION + prev_pointer_offset)) = item;
  }

  inline void SetInsertCommit(const oid_t &tuple_slot_id,
                              const bool commit) const {
    *((bool *)(TUPLE_HEADER_LOCATION + insert_commit_offset)) = commit;
  }

  inline void SetDeleteCommit(const oid_t &tuple_slot_id,
                              const bool commit) const {
    *((bool *)(TUPLE_HEADER_LOCATION + delete_commit_offset)) = commit;
  }

  // Getters for addresses
  inline txn_id_t *GetTransactionIdLocation(const oid_t &tuple_slot_id) const {
    return ((txn_id_t *)(TUPLE_HEADER_LOCATION));
  }

  inline txn_id_t SetAtomicTransactionId(const oid_t &tuple_slot_id, const txn_id_t &old_txn_id, const txn_id_t &new_txn_id) const {
    txn_id_t *txn_id_ptr = (txn_id_t *)(TUPLE_HEADER_LOCATION);
    return __sync_val_compare_and_swap(txn_id_ptr, old_txn_id, new_txn_id);
  }

  inline bool SetAtomicTransactionId(const oid_t &tuple_slot_id,
                            const txn_id_t &transaction_id) const {
    txn_id_t *txn_id_ptr = (txn_id_t *)(TUPLE_HEADER_LOCATION);
    return __sync_bool_compare_and_swap(txn_id_ptr, INITIAL_TXN_ID, transaction_id);
  }

  void PrintVisibility(txn_id_t txn_id, cid_t at_cid);

  // Sync the contents
  void Sync();

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

 // *  -----------------------------------------------------------------------------
 // *  | TxnID (8 bytes)  | BeginTimeStamp (8 bytes) | EndTimeStamp (8 bytes) |
 // *  | NextItemPointer (16 bytes) | PrevItemPointer (16 bytes) | ReservedField (24 bytes)
 // *  | InsertCommit (1 byte) | DeleteCommit (1 byte)
 // *  -----------------------------------------------------------------------------

 private:
  // header entry size is the size of the layout described above
  static const size_t header_entry_size = sizeof(txn_id_t) + 2 * sizeof(cid_t) +
                                          2 * sizeof(ItemPointer) + 24 +
                                          2 * sizeof(bool);
  static const size_t txn_id_offset = 0;
  static const size_t begin_cid_offset = sizeof(txn_id_t);
  static const size_t end_cid_offset = begin_cid_offset + sizeof(cid_t);
  static const size_t next_pointer_offset = end_cid_offset + sizeof(cid_t);
  static const size_t prev_pointer_offset = next_pointer_offset + sizeof(ItemPointer);
  static const size_t reserved_field_offset = prev_pointer_offset + sizeof(ItemPointer);
  static const size_t insert_commit_offset = reserved_field_offset + 24;
  static const size_t delete_commit_offset =
      insert_commit_offset + sizeof(bool);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Backend
  BackendType backend_type;

  size_t header_size;

  // set of fixed-length tuple slots
  char *data;

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // next free tuple slot
  std::atomic<oid_t> next_tuple_slot;

  // synch helpers
  Spinlock tile_header_lock;
};

}  // End storage namespace
}  // End peloton namespace
