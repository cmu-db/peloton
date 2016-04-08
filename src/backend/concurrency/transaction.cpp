//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.cpp
//
// Identification: src/backend/concurrency/transaction.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/concurrency/transaction.h"

#include "backend/common/logger.h"
#include "backend/common/platform.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace peloton {
namespace concurrency {

void Transaction::RecordRead(const oid_t &tile_group_id,
                             const oid_t &tuple_id) {
  if (rw_set_.find(tile_group_id) != rw_set_.end() &&
      rw_set_.at(tile_group_id).find(tuple_id) !=
          rw_set_.at(tile_group_id).end()) {
    assert(rw_set_.at(tile_group_id).at(tuple_id) != RW_TYPE_DELETE &&
           rw_set_.at(tile_group_id).at(tuple_id) != RW_TYPE_INS_DEL);
    return;
  } else {
    rw_set_[tile_group_id][tuple_id] = RW_TYPE_READ;
  }
}

void Transaction::RecordUpdate(const oid_t &tile_group_id,
                               const oid_t &tuple_id) {
  if (rw_set_.find(tile_group_id) != rw_set_.end() &&
      rw_set_.at(tile_group_id).find(tuple_id) !=
          rw_set_.at(tile_group_id).end()) {
    RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    if (type == RW_TYPE_READ) {
      type = RW_TYPE_UPDATE;
      // record write.
      is_written_ = true;
      return;
    }
    if (type == RW_TYPE_UPDATE) {
      return;
    }
    if (type == RW_TYPE_INSERT) {
      return;
    }
    if (type == RW_TYPE_DELETE) {
      assert(false);
      return;
    }
    assert(false);
  } else {
    assert(false);
  }
}

void Transaction::RecordInsert(const oid_t &tile_group_id,
                               const oid_t &tuple_id) {
  if (rw_set_.find(tile_group_id) != rw_set_.end() &&
      rw_set_.at(tile_group_id).find(tuple_id) !=
          rw_set_.at(tile_group_id).end()) {
    // RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    assert(false);
  } else {
    rw_set_[tile_group_id][tuple_id] = RW_TYPE_INSERT;
    ++insert_count_;
  }
}

void Transaction::RecordDelete(const oid_t &tile_group_id,
                               const oid_t &tuple_id) {
  if (rw_set_.find(tile_group_id) != rw_set_.end() &&
      rw_set_.at(tile_group_id).find(tuple_id) !=
          rw_set_.at(tile_group_id).end()) {
    RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    if (type == RW_TYPE_READ) {
      type = RW_TYPE_DELETE;
      // record write.
      is_written_ = true;
      return;
    }
    if (type == RW_TYPE_UPDATE) {
      type = RW_TYPE_DELETE;
      return;
    }
    if (type == RW_TYPE_INSERT) {
      type = RW_TYPE_INS_DEL;
      --insert_count_;
      return;
    }
    if (type == RW_TYPE_DELETE) {
      assert(false);
      return;
    }
    assert(false);
  } else {
    assert(false);
  }
}

void Transaction::RecordRead(const ItemPointer &location) {
  RecordRead(location.block, location.offset);
}

void Transaction::RecordUpdate(const ItemPointer &location) {
  RecordUpdate(location.block, location.offset);
}

void Transaction::RecordInsert(const ItemPointer &location) {
  RecordInsert(location.block, location.offset);
}

void Transaction::RecordDelete(const ItemPointer &location) {
  RecordDelete(location.block, location.offset);
}

const std::map<oid_t, std::map<oid_t, RWType>> &Transaction::GetRWSet() {
  return rw_set_;
}

const std::string Transaction::GetInfo() const {
  std::ostringstream os;

  os << "\tTxn :: @" << this << " ID : " << std::setw(4) << txn_id_
     << " Begin Commit ID : " << std::setw(4) << begin_cid_
     << " End Commit ID : " << std::setw(4) << end_cid_
     << " Result : " << result_;

  return os.str();
}

}  // End concurrency namespace
}  // End peloton namespace
