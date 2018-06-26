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
// Tuple Header
//===--------------------------------------------------------------------===//

struct TupleHeader {
  common::synchronization::SpinLatch latch;
  std::atomic<txn_id_t> txn_id;
  cid_t read_ts;
  cid_t begin_ts;
  cid_t end_ts;
  ItemPointer next;
  ItemPointer prev;
  ItemPointer *indirection;
} __attribute__((aligned(64)));

/**
 *  FIELD DESCRIPTIONS:
 *  ===================
 *  latch: Tuple header latch used to acquire ownership or update read_ts
 *  txn_id: serve as a write lock on the tuple version
 *  read_ts: the last txn to read this tuple
 *  begin_ts: the lower bound of the version visibility range.
 *  end_ts: the upper bound of the version visibility range.
 *  next: the pointer pointing to the next (older) version in the version chain.
 *  prev: the pointer pointing to the prev (newer) version in the version chain.
 *  indirection: the pointer pointing to the index entry that holds the address of the version chain header.
*/

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

/**
 *
 * This contains information related to MVCC.
 * It is shared by all tiles in a tile group.
 *
 *  STATUS:
 *  ===================
 *  TxnID == INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> empty version
 *  TxnID != INITIAL_TXN_ID, BeginTS != MAX_CID --> to-be-updated old version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> to-be-installed new version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == INVALID_CID --> to-be-installed deleted version
 */

class TileGroupHeader : public Printable {
  TileGroupHeader() = delete;

 public:
  TileGroupHeader(const BackendType &backend_type, const int &tuple_count);

  TileGroupHeader &operator=(const peloton::storage::TileGroupHeader &other) {
    // check for self-assignment
    if (&other == this) return *this;

    backend_type = other.backend_type;
    tile_group = other.tile_group;
    num_tuple_slots = other.num_tuple_slots;
    next_tuple_slot.store(other.next_tuple_slot);
    immutable = other.immutable;

    // copy tuple header values
    for (oid_t tuple_slot_id = START_OID; tuple_slot_id < num_tuple_slots;
         tuple_slot_id++) {
      SetTransactionId(tuple_slot_id, other.GetTransactionId(tuple_slot_id));
      SetLastReaderCommitId(tuple_slot_id,
                            other.GetLastReaderCommitId(tuple_slot_id));
      SetBeginCommitId(tuple_slot_id, other.GetBeginCommitId(tuple_slot_id));
      SetEndCommitId(tuple_slot_id, other.GetEndCommitId(tuple_slot_id));
      SetNextItemPointer(tuple_slot_id,
                         other.GetNextItemPointer(tuple_slot_id));
      SetPrevItemPointer(tuple_slot_id,
                         other.GetPrevItemPointer(tuple_slot_id));
      SetIndirection(tuple_slot_id, other.GetIndirection(tuple_slot_id));
    }

    return *this;
  }

  ~TileGroupHeader() = default;

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
    PELOTON_ASSERT(tile_group);
    return tile_group;
  }

  inline common::synchronization::SpinLatch &GetSpinLatch(
      const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].latch;
  }

  inline txn_id_t GetTransactionId(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].txn_id;
  }

  inline cid_t GetLastReaderCommitId(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].read_ts;
  }

  inline cid_t GetBeginCommitId(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].begin_ts;
  }

  inline cid_t GetEndCommitId(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].end_ts;
  }

  inline ItemPointer GetNextItemPointer(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].next;
  }

  inline ItemPointer GetPrevItemPointer(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].prev;
  }

  inline ItemPointer *GetIndirection(const oid_t &tuple_slot_id) const {
    return tuple_headers_[tuple_slot_id].indirection;
  }

  // Setters

  inline void SetTileGroup(TileGroup *tile_group) {
    this->tile_group = tile_group;
  }

  inline void SetTransactionId(const oid_t &tuple_slot_id,
                               const txn_id_t &transaction_id) const {
    tuple_headers_[tuple_slot_id].txn_id = transaction_id;
  }

  inline void SetLastReaderCommitId(const oid_t &tuple_slot_id,
                                    const cid_t &read_cid) const {
    tuple_headers_[tuple_slot_id].read_ts = read_cid;
  }

  inline void SetBeginCommitId(const oid_t &tuple_slot_id,
                               const cid_t &begin_cid) {
    tuple_headers_[tuple_slot_id].begin_ts = begin_cid;
  }

  inline void SetEndCommitId(const oid_t &tuple_slot_id,
                             const cid_t &end_cid) const {
    tuple_headers_[tuple_slot_id].end_ts = end_cid;
  }

  inline void SetNextItemPointer(const oid_t &tuple_slot_id,
                                 const ItemPointer &item) const {
    tuple_headers_[tuple_slot_id].next = item;
  }

  inline void SetPrevItemPointer(const oid_t &tuple_slot_id,
                                 const ItemPointer &item) const {
    tuple_headers_[tuple_slot_id].prev = item;
  }

  inline void SetIndirection(const oid_t &tuple_slot_id,
                             ItemPointer *indirection) const {
    tuple_headers_[tuple_slot_id].indirection = indirection;
  }

  inline bool SetAtomicTransactionId(const oid_t &tuple_slot_id,
                                     const txn_id_t &transaction_id) const {
    auto old_val = INITIAL_TXN_ID;
    return tuple_headers_[tuple_slot_id].txn_id.compare_exchange_strong(
        old_val, transaction_id);
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

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Backend
  BackendType backend_type;

  // Associated tile_group
  TileGroup *tile_group;

  std::unique_ptr<TupleHeader[]> tuple_headers_;

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // next free tuple slot
  // WARNING: this variable may not be the right boundary of the tile
  // IT MAY OUT OF BOUNDARY! ALWAYS CHECK IF IT EXCEEDS num_tuple_slots
  std::atomic<oid_t> next_tuple_slot;

  common::synchronization::SpinLatch tile_header_lock;

  // Immmutable Flag. Should be set by the indextuner to be true.
  // By default it will be set to false.
  bool immutable;
};

}  // namespace storage
}  // namespace peloton
