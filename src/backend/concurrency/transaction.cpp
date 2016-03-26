//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction.cpp
//
// Identification: src/backend/concurrency/transaction.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
  if (rw_set.find(tile_group_id) != rw_set.end() &&
      rw_set.at(tile_group_id).find(tuple_id) !=
          rw_set.at(tile_group_id).end()) {
    // RWType &type = rw_set.at(tile_group_id).at(tuple_id);
    assert(rw_set.at(tile_group_id).at(tuple_id) != RW_TYPE_DELETE &&
           rw_set.at(tile_group_id).at(tuple_id) != RW_TYPE_INS_DEL);
    return;
  } else {
    rw_set[tile_group_id][tuple_id] = RW_TYPE_READ;
  }
}

void Transaction::RecordWrite(const oid_t &tile_group_id,
                              const oid_t &tuple_id) {
  if (rw_set.find(tile_group_id) != rw_set.end() &&
      rw_set.at(tile_group_id).find(tuple_id) !=
          rw_set.at(tile_group_id).end()) {
    RWType &type = rw_set.at(tile_group_id).at(tuple_id);
    if (type == RW_TYPE_READ) {
      type = RW_TYPE_UPDATE;
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
    rw_set[tile_group_id][tuple_id] = RW_TYPE_UPDATE;
  }
}

void Transaction::RecordInsert(const oid_t &tile_group_id,
                               const oid_t &tuple_id) {
  if (rw_set.find(tile_group_id) != rw_set.end() &&
      rw_set.at(tile_group_id).find(tuple_id) !=
          rw_set.at(tile_group_id).end()) {
    // RWType &type = rw_set.at(tile_group_id).at(tuple_id);
    assert(false);
  } else {
    rw_set[tile_group_id][tuple_id] = RW_TYPE_INSERT;
  }
}

void Transaction::RecordDelete(const oid_t &tile_group_id,
                               const oid_t &tuple_id) {
  if (rw_set.find(tile_group_id) != rw_set.end() &&
      rw_set.at(tile_group_id).find(tuple_id) !=
          rw_set.at(tile_group_id).end()) {
    RWType &type = rw_set.at(tile_group_id).at(tuple_id);
    if (type == RW_TYPE_READ) {
      type = RW_TYPE_DELETE;
      return;
    }
    if (type == RW_TYPE_UPDATE) {
      type = RW_TYPE_DELETE;
      return;
    }
    if (type == RW_TYPE_INSERT) {
      type = RW_TYPE_INS_DEL;
      return;
    }
    if (type == RW_TYPE_DELETE) {
      assert(false);
      return;
    }
    assert(false);
  } else {
    rw_set[tile_group_id][tuple_id] = RW_TYPE_DELETE;
  }
}

void Transaction::RecordRead(const ItemPointer &location) {
  RecordRead(location.block, location.offset);
}

void Transaction::RecordWrite(const ItemPointer &location) {
  RecordWrite(location.block, location.offset);
}

void Transaction::RecordInsert(const ItemPointer &location) {
  RecordInsert(location.block, location.offset);
}

void Transaction::RecordDelete(const ItemPointer &location) {
  RecordDelete(location.block, location.offset);
}

const std::map<oid_t, std::map<oid_t, RWType>> &Transaction::GetRWSet() {
  return rw_set;
}

void Transaction::ResetState(void) {
  rw_set.clear();
}

const std::string Transaction::GetInfo() const {
  std::ostringstream os;

  os << "\tTxn :: @" << this << " ID : " << std::setw(4) << txn_id
     << " Start Commit ID : " << std::setw(4) << start_cid
     << " End Commit ID : " << std::setw(4) << end_cid
     << " Result : " << result_;

  if (next == nullptr) {
    os << " Next : " << std::setw(4) << next;
  } else {
    os << " Next : " << std::setw(4) << next->txn_id;
  }

  os << " Ref count : " << std::setw(4) << ref_count << "\n";
  return os.str();
}

}  // End concurrency namespace
}  // End peloton namespace
