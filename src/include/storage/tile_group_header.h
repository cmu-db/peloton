//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.h
//
// Identification: src/include/storage/tile_group_header.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <cstring>

#include "common/item_pointer.h"
#include "common/macros.h"
#include "common/synchronization/spin_latch.h"
#include "common/printable.h"
#include "storage/tuple.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace storage {

class TileGroup;

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

/**
 *
 * This contains information related to MVCC.
 * It is shared by all tiles in a tile group.
 *
 *  Layout :
 *
 *  -----------------------------------------------------------------------------
 *  | TxnID (8 bytes)  | BeginTimeStamp (8 bytes) | EndTimeStamp (8 bytes) |
 *  | NextItemPointer (8 bytes) | PrevItemPointer (8 bytes) |
 *  | Indirection (8 bytes) | ReservedField (16 bytes)
 *  -----------------------------------------------------------------------------
 *
 *  FIELD DESCRIPTIONS:
 *  ===================
 *  TxnID: serve as a write lock on the tuple version.
 *  BeginTimeStamp: the lower bound of the version visibility range.
 *  EndTimeStamp: the upper bound of the version visibility range.
 *  NextItemPointer: the pointer pointing to the next (older) version in the version chain.
 *  PrevItemPointer: the pointer pointing to the prev (newer) version in the version chain.
 *  Indirection: the pointer pointing to the index entry that holds the address of the version chain header.
 *  ReservedField: unused space for future usage.
 *
 *  STATUS:
 *  ===================
 *  TxnID == INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> empty version
 *  TxnID != INITIAL_TXN_ID, BeginTS != MAX_CID --> to-be-updated old version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> to-be-installed new version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == INVALID_CID --> to-be-installed deleted version
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
    PL_MEMCPY(data, other.data, header_size);

    num_tuple_slots = other.num_tuple_slots;
    oid_t val = other.next_tuple_slot;
    next_tuple_slot = val;

    return *this;
  }

  ~TileGroupHeader();

  // this function is only called by DataTable::GetEmptyTupleSlot().
  oid_t GetNextEmptyTupleSlot() {
    if (next_tuple_slot >= num_tuple_slots) {
      return INVALID_OID;
    }

    oid_t tuple_slot_id =
        next_tuple_slot.fetch_add(1, std::memory_order_relaxed);

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

  oid_t GetCurrentNextTupleSlot() const {
    // Carefully check if the next_tuple_slot is out of boundary
    oid_t next_tid = next_tuple_slot;
    if (next_tid < num_tuple_slots) {
      return next_tid;
    } else {
      return num_tuple_slots;
    }
  }

  oid_t GetActiveTupleCount() const;

  //===--------------------------------------------------------------------===//
  // MVCC utilities
  //===--------------------------------------------------------------------===//

  // Getters
  inline const TileGroup *GetTileGroup() const {
    PL_ASSERT(tile_group);
    return tile_group;
  }

  // it is possible that some other transactions are modifying the txn_id,
  // but the current transaction reads the txn_id.
  // the returned value seems to be uncertain.
  inline txn_id_t GetTransactionId(const oid_t &tuple_slot_id) const {
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

  inline ItemPointer *GetIndirection(const oid_t &tuple_slot_id) const {
    return *(ItemPointer **)(TUPLE_HEADER_LOCATION + indirection_offset);
  }

  // constraint: at most 16 bytes.
  inline char *GetReservedFieldRef(const oid_t &tuple_slot_id) const {
    return (char *)(TUPLE_HEADER_LOCATION + reserved_field_offset);
  }

  // Setters

  inline void SetTileGroup(TileGroup *tile_group) {
    this->tile_group = tile_group;
  }
  inline void SetTransactionId(const oid_t &tuple_slot_id,
                               const txn_id_t &transaction_id) const {
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

  inline void SetIndirection(const oid_t &tuple_slot_id,
                             const ItemPointer *indirection) const {
    *((const ItemPointer **)(TUPLE_HEADER_LOCATION + indirection_offset)) =
        indirection;
  }

  inline txn_id_t SetAtomicTransactionId(const oid_t &tuple_slot_id,
                                         const txn_id_t &old_txn_id,
                                         const txn_id_t &new_txn_id) const {
    txn_id_t *txn_id_ptr = (txn_id_t *)(TUPLE_HEADER_LOCATION);
    return __sync_val_compare_and_swap(txn_id_ptr, old_txn_id, new_txn_id);
  }

  inline bool SetAtomicTransactionId(const oid_t &tuple_slot_id,
                                     const txn_id_t &transaction_id) const {
    txn_id_t *txn_id_ptr = (txn_id_t *)(TUPLE_HEADER_LOCATION);
    return __sync_bool_compare_and_swap(txn_id_ptr, INITIAL_TXN_ID,
                                        transaction_id);
  }

  /*
  * @brief The following method use Compare and Swap to set the tilegroup's
  immutable flag to be true. 
  */
  inline bool SetImmutability() {
    return __sync_bool_compare_and_swap(&immutable, false, true);
  }
  
  /*
  * @brief The following method use Compare and Swap to set the tilegroup's
  immutable flag to be false. 
  */
  inline bool ResetImmutability() {
    return __sync_bool_compare_and_swap(&immutable, true, false);
  }

  inline bool GetImmutability() const { return immutable; }

  void PrintVisibility(txn_id_t txn_id, cid_t at_cid);

  // Getter for spin lock
  common::synchronization::SpinLatch &GetHeaderLock() { return tile_header_lock; }

  // Sync the contents
  void Sync();

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  static inline size_t GetReservedSize() { return reserved_size; }

  // header entry size is the size of the layout described above
  static const size_t reserved_size = 16;
  static const size_t header_entry_size = sizeof(txn_id_t) + 2 * sizeof(cid_t) +
                                          2 * sizeof(ItemPointer) +
                                          sizeof(ItemPointer *) + reserved_size;
  static const size_t txn_id_offset = 0;
  static const size_t begin_cid_offset = txn_id_offset + sizeof(txn_id_t);
  static const size_t end_cid_offset = begin_cid_offset + sizeof(cid_t);
  static const size_t next_pointer_offset = end_cid_offset + sizeof(cid_t);
  static const size_t prev_pointer_offset =
      next_pointer_offset + sizeof(ItemPointer);
  static const size_t indirection_offset =
      prev_pointer_offset + sizeof(ItemPointer);
  static const size_t reserved_field_offset =
      indirection_offset + sizeof(ItemPointer);

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Backend
  BackendType backend_type;

  // Associated tile_group
  TileGroup *tile_group;

  size_t header_size;

  // set of fixed-length tuple slots
  char *data;

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // next free tuple slot
  // WARNING: this variable may not be the right boundary of the tile
  // IT MAY OUT OF BOUNDARY! ALWAYS CHECK IF IT EXCEEDS num_tuple_slots
  std::atomic<oid_t> next_tuple_slot;

  common::synchronization::SpinLatch tile_header_lock;

  // Immmutable Flag. Should be set by the brain to be true.
  // By default it will be set to false.
  bool immutable;
};

}  // namespace storage
}  // namespace peloton
