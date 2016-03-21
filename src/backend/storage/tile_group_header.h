//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group_header.h
//
// Identification: src/backend/storage/tile_group_header.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
 * 	-----------------------------------------------------------------------------
 *  | Txn ID (8 bytes)  | Begin TimeStamp (8 bytes) | End TimeStamp (8 bytes) |
 *  | InsertCommit (1 byte) | DeleteCommit (1 byte) | Prev ItemPointer (16 bytes)
 * 	-----------------------------------------------------------------------------
 *
 */

class TileGroupHeader : public Printable {
  TileGroupHeader() = delete;

 public:
  TileGroupHeader(BackendType backend_type, int tuple_count);

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

  oid_t GetNextEmptyTupleSlot() {
    oid_t tuple_slot_id = INVALID_OID;

    {
      std::lock_guard<std::mutex> tile_header_lock(tile_header_mutex);

      // check tile group capacity
      if (next_tuple_slot < num_tuple_slots) {
        tuple_slot_id = next_tuple_slot;
        next_tuple_slot++;
      }
    }

    return tuple_slot_id;
  }

  /**
   * Used by logging
   */
  bool GetEmptyTupleSlot(oid_t tuple_slot_id) {
    {
      std::lock_guard<std::mutex> tile_header_lock(tile_header_mutex);

      if (tuple_slot_id < num_tuple_slots) {
        if (next_tuple_slot <= tuple_slot_id) {
          next_tuple_slot = tuple_slot_id + 1;
        }
        return true;
      } else {
        return false;
      }
    }
  }

  oid_t GetNextTupleSlot() const { return next_tuple_slot; }

  oid_t GetActiveTupleCount(txn_id_t txn_id);

  //===--------------------------------------------------------------------===//
  // MVCC utilities
  //===--------------------------------------------------------------------===//

  // Getters

  inline txn_id_t GetTransactionId(const oid_t tuple_slot_id) const {
    return *((txn_id_t *)(data + (tuple_slot_id * header_entry_size)));
  }

  inline cid_t GetBeginCommitId(const oid_t tuple_slot_id) const {
    return *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
                       begin_cid_offset));
  }

  inline cid_t GetEndCommitId(const oid_t tuple_slot_id) const {
    return *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
                       end_cid_offset));
  }

  inline bool GetInsertCommit(const oid_t tuple_slot_id) const {
    return *((bool *)(data + (tuple_slot_id * header_entry_size) +
                      insert_commit_offset));
  }

  inline bool GetDeleteCommit(const oid_t tuple_slot_id) const {
    return *((bool *)(data + (tuple_slot_id * header_entry_size) +
                      delete_commit_offset));
  }

  inline ItemPointer GetPrevItemPointer(const oid_t tuple_slot_id) const {
    return *((ItemPointer *)(data + (tuple_slot_id * header_entry_size) +
                      pointer_offset));
  }

  // Setters
  inline void SetTransactionId(const oid_t tuple_slot_id,
                               txn_id_t transaction_id) {
    *((txn_id_t *)(data + (tuple_slot_id * header_entry_size))) =
        transaction_id;
  }

  inline void SetBeginCommitId(const oid_t tuple_slot_id, cid_t begin_cid) {
    *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
                begin_cid_offset)) = begin_cid;
  }

  inline void SetEndCommitId(const oid_t tuple_slot_id, cid_t end_cid) const {
    *((cid_t *)(data + (tuple_slot_id * header_entry_size) +
                end_cid_offset)) = end_cid;
  }

  inline void SetInsertCommit(const oid_t tuple_slot_id, bool commit) const {
    *((bool *)(data + (tuple_slot_id * header_entry_size) +
                insert_commit_offset)) = commit;
  }

  inline void SetDeleteCommit(const oid_t tuple_slot_id, bool commit) const {
    *((bool *)(data + (tuple_slot_id * header_entry_size) +
                delete_commit_offset)) = commit;
  }

  inline void SetPrevItemPointer(const oid_t tuple_slot_id,
                                 ItemPointer item) const {
    *((ItemPointer *)(data + (tuple_slot_id * header_entry_size) +
                      pointer_offset)) = item;
  }

  // Getters for addresses
  inline txn_id_t *GetTransactionIdLocation(const oid_t tuple_slot_id) const {
    return ((txn_id_t *)(data + (tuple_slot_id * header_entry_size)));
  }

  inline bool LatchTupleSlot(const oid_t tuple_slot_id,
                             txn_id_t transaction_id) {
    txn_id_t *txn_id = (txn_id_t *)(data + (tuple_slot_id * header_entry_size));
    if (atomic_cas(txn_id, INITIAL_TXN_ID, transaction_id)) {
      return true;
    } else {
      return false;
    }
  }

  inline bool ReleaseTupleSlot(const oid_t tuple_slot_id,
                               txn_id_t transaction_id) {
    txn_id_t *txn_id = (txn_id_t *)(data + (tuple_slot_id * header_entry_size));
    if (!atomic_cas(txn_id, transaction_id, INITIAL_TXN_ID)) {
      LOG_INFO("Release failed, expecting a deleted own insert: %lu",
               GetTransactionId(tuple_slot_id));
      assert(GetTransactionId(tuple_slot_id) == INVALID_TXN_ID);
      return false;
    }
    return true;
  }

  inline bool ReleaseDeleteTupleSlot(const oid_t tuple_slot_id,
                               txn_id_t transaction_id) {
    txn_id_t *txn_id = (txn_id_t *)(data + (tuple_slot_id * header_entry_size));
    if (!atomic_cas(txn_id, transaction_id, INVALID_TXN_ID)) {
      LOG_INFO("Release failed, expecting a deleted own insert: %lu",
               GetTransactionId(tuple_slot_id));
      assert(GetTransactionId(tuple_slot_id) == INVALID_TXN_ID);
      return false;
    }
    return true;
  }


    // Visibility check
  bool IsVisible(const oid_t tuple_slot_id, txn_id_t txn_id, cid_t at_lcid) {
    txn_id_t tuple_txn_id = GetTransactionId(tuple_slot_id);
    cid_t tuple_begin_cid = GetBeginCommitId(tuple_slot_id);
    cid_t tuple_end_cid = GetEndCommitId(tuple_slot_id);

    if (tuple_txn_id == INVALID_TXN_ID) {
      // the tuple is not available.
      return false;
    }
    bool own = (txn_id == tuple_txn_id);

    // there are exactly two versions that can be owned by a transaction.
    if (own == true) {
      if (tuple_begin_cid == MAX_CID) {
        // the only version that is visible is the newly inserted one.
        return true;
      } else {
        // the older version is not visible.
        return false;
      }
    }
    else {
      bool activated = (at_lcid >= tuple_begin_cid);
      bool invalidated = (at_lcid >= tuple_end_cid);
      if (tuple_txn_id != INITIAL_TXN_ID) {
        // if the tuple is owned by other transactions.
        if (tuple_begin_cid == MAX_CID) {
          // currently, we do not handle cascading abort. so never read an uncommitted version.
          return false;
        } else {
          // the older version may be visible.
          if (activated && !invalidated) {
            return true;
          } else{
            return false;
          }
        }
      } else {
        // if the tuple is not owned by any transaction.
        if (activated && !invalidated) {
          return true;
        }
        else {
          return false;
        }
      }
    }

//    // Own
//    LOG_TRACE("Own :: %d txn id : %lu tuple txn id : %lu", own, txn_id,
//              tuple_txn_id);
//
//    // Activated
//    if (tuple_begin_cid == MAX_CID) { // ?????
//      LOG_TRACE("Activated :: %d cid : %lu tuple begin cid : MAX_CID",
//                activated, at_lcid);
//    } else {
//      LOG_TRACE("Activated :: %d , lcid : %lu , tuple begin cid : %lu",
//                activated, at_lcid, tuple_begin_cid);
//    }
//
//    // Invalidated
//    if (tuple_end_cid == MAX_CID) {
//      LOG_TRACE("Invalidated:: %d cid : %lu tuple end cid : MAX_CID",
//                invalidated, at_lcid);
//    } else {
//      LOG_TRACE("Invalidated:: %d cid : %lu tuple end cid : %lu", invalidated,
//                at_lcid, tuple_end_cid);
//    }
//
//    // overwrite activated/invalidated if using peloton logging
//    {
//      auto &log_manager = logging::LogManager::GetInstance();
//      if (log_manager.HasPelotonFrontendLogger() == LOGGING_TYPE_NVM_NVM) {
//        bool insert_commit = GetInsertCommit(tuple_slot_id);
//        bool delete_commit = GetDeleteCommit(tuple_slot_id);
//
//        activated = activated && insert_commit;
//        invalidated = invalidated && delete_commit;
//      }
//    }
//
//    // Visible iff past Insert || Own Insert
//    bool visible = !(tuple_txn_id == INVALID_TXN_ID) &&
//                   ((!own && activated && !invalidated) ||
//                    (own && !activated && !invalidated));
//
//    LOG_INFO(
//        "<%p, %lu> :(vtid, vbeg, vend) = (%lu, %lu, %lu), (tid, lcid) = (%lu, "
//        "%lu), visible = %d",
//        this, tuple_slot_id, tuple_txn_id, tuple_begin_cid, tuple_end_cid,
//        txn_id, at_lcid, visible);
//
//    return visible;
  }

  /**
   * This is called after latching
   */
  bool IsDeletable(const oid_t tuple_slot_id,
                   __attribute__((unused)) txn_id_t txn_id,
                   __attribute__((unused)) cid_t at_lcid) {
    cid_t tuple_end_cid = GetEndCommitId(tuple_slot_id);

    bool deletable = tuple_end_cid == MAX_CID;

    LOG_INFO(
        "<%p, %lu> :(vtid, vbeg, vend) = (%lu, %lu, %lu), (tid, lcid) = (%lu, "
        "%lu), deletable = %d",
        this, tuple_slot_id, GetTransactionId(tuple_slot_id),
        GetBeginCommitId(tuple_slot_id), tuple_end_cid, txn_id, at_lcid,
        deletable);

    return deletable;
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
 // *  | Txn ID (8 bytes)  | Begin TimeStamp (8 bytes) | End TimeStamp (8 bytes) |
 // *--
 // *  |InsertCommit (1 byte) | DeleteCommit (1 byte) | Prev ItemPointer (16 bytes)
 // *| lock bit (1 byte)


 private:
  // header entry size is the size of the layout described above
  static const size_t header_entry_size = sizeof(txn_id_t) + 2 * sizeof(cid_t) +
                                          2 * sizeof(bool) +
                                          sizeof(ItemPointer);
  static const size_t txn_id_offset = 0;
  static const size_t begin_cid_offset = sizeof(txn_id_t);
  static const size_t end_cid_offset = begin_cid_offset + sizeof(cid_t);
  static const size_t insert_commit_offset = end_cid_offset + sizeof(cid_t);
  static const size_t delete_commit_offset = insert_commit_offset + sizeof(bool);
  static const size_t pointer_offset = delete_commit_offset + sizeof(bool);

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
  oid_t next_tuple_slot;

  // synch helpers
  std::mutex tile_header_mutex;
};

}  // End storage namespace
}  // End peloton namespace
