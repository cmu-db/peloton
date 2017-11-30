//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.cpp
//
// Identification: src/concurrency/transaction.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction.h"

#include <sstream>

#include "common/logger.h"
#include "common/platform.h"
#include "common/macros.h"
#include "trigger/trigger.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace peloton {
namespace concurrency {

/*
 * Transaction state transition:
 *                r           r/ro            u/r/ro
 *              +--<--+     +---<--+        +---<--+
 *           r  |     |     |      |        |      |     d
 *  (init)-->-- +-> Read  --+-> Read Own ---+--> Update ---> Delete (final)
 *                    |   ro             u  |
 *                    |                     |
 *                    +----->--------->-----+
 *                              u
 *              r/ro/u
 *            +---<---+
 *         i  |       |     d
 *  (init)-->-+---> Insert ---> Ins_Del (final)
 *
 *    r : read
 *    ro: read_own
 *    u : update
 *    d : delete
 *    i : insert
 */

Transaction::Transaction(const size_t thread_id,
                         const IsolationLevelType isolation,
                         const cid_t &read_id) {
  Init(thread_id, isolation, read_id);
}

Transaction::Transaction(const size_t thread_id,
                         const IsolationLevelType isolation,
                         const cid_t &read_id, const cid_t &commit_id) {
  Init(thread_id, isolation, read_id, commit_id);
}

Transaction::~Transaction() {}

void Transaction::Init(const size_t thread_id,
                       const IsolationLevelType isolation, const cid_t &read_id,
                       const cid_t &commit_id) {
  read_id_ = read_id;

  // commit id can be set at a transaction's commit phase.
  commit_id_ = commit_id;

  // set txn_id to commit_id.
  txn_id_ = commit_id_;

  epoch_id_ = read_id_ >> 32;

  thread_id_ = thread_id;

  isolation_level_ = isolation;

  is_written_ = false;

  insert_count_ = 0;

  gc_set_.reset(new GCSet());
  gc_object_set_.reset(new GCObjectSet());

  on_commit_triggers_.reset();
}

RWType Transaction::GetRWType(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;
  auto itr = rw_set_.find(tile_group_id);
  if (itr == rw_set_.end()) {
    return RWType::INVALID;
  }

  auto inner_itr = itr->second.find(tuple_id);
  if (inner_itr == itr->second.end()) {
    return RWType::INVALID;
  }

  return inner_itr->second;
}

void Transaction::RecordRead(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  if (IsInRWSet(location)) {
    PL_ASSERT(rw_set_.at(tile_group_id).at(tuple_id) != RWType::DELETE &&
              rw_set_.at(tile_group_id).at(tuple_id) != RWType::INS_DEL);
    return;
  } else {
    rw_set_[tile_group_id][tuple_id] = RWType::READ;
  }
}

void Transaction::RecordReadOwn(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  if (IsInRWSet(location)) {
    RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    if (type == RWType::READ) {
      type = RWType::READ_OWN;
      // record write.
      return;
    }
    PL_ASSERT(type != RWType::DELETE && type != RWType::INS_DEL);
  } else {
    rw_set_[tile_group_id][tuple_id] = RWType::READ_OWN;
  }
}

void Transaction::RecordUpdate(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  if (IsInRWSet(location)) {
    RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    if (type == RWType::READ || type == RWType::READ_OWN) {
      type = RWType::UPDATE;
      // record write.
      is_written_ = true;

      return;
    }
    if (type == RWType::UPDATE) {
      return;
    }
    if (type == RWType::INSERT) {
      return;
    }
    if (type == RWType::DELETE) {
      PL_ASSERT(false);
      return;
    }
    PL_ASSERT(false);
  } else {
    // consider select_for_udpate case.
    rw_set_[tile_group_id][tuple_id] = RWType::UPDATE;
  }
}

void Transaction::RecordInsert(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  if (IsInRWSet(location)) {
    PL_ASSERT(false);
  } else {
    rw_set_[tile_group_id][tuple_id] = RWType::INSERT;
    ++insert_count_;
  }
}

bool Transaction::RecordDelete(const ItemPointer &location) {
  std::lock_guard<std::mutex> lock(mu_);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  if (IsInRWSet(location)) {
    RWType &type = rw_set_.at(tile_group_id).at(tuple_id);
    if (type == RWType::READ || type == RWType::READ_OWN) {
      type = RWType::DELETE;
      // record write.
      is_written_ = true;

      return false;
    }
    if (type == RWType::UPDATE) {
      type = RWType::DELETE;

      return false;
    }
    if (type == RWType::INSERT) {
      type = RWType::INS_DEL;
      --insert_count_;

      return true;
    }
    if (type == RWType::DELETE) {
      PL_ASSERT(false);
      return false;
    }
    PL_ASSERT(false);
  } else {
    rw_set_[tile_group_id][tuple_id] = RWType::DELETE;
  }
  return false;
}

const std::string Transaction::GetInfo() const {
  std::ostringstream os;

  os << "\tTxn :: @" << this << " ID : " << std::setw(4) << txn_id_
     << " Read ID : " << std::setw(4) << read_id_
     << " Commit ID : " << std::setw(4) << commit_id_
     << " Result : " << result_;

  return os.str();
}

void Transaction::AddOnCommitTrigger(trigger::TriggerData &trigger_data) {
  if (on_commit_triggers_ == nullptr) {
    on_commit_triggers_.reset(new trigger::TriggerSet());
  }
  on_commit_triggers_->push_back(trigger_data);
}

void Transaction::ExecOnCommitTriggers() {
  if (on_commit_triggers_ != nullptr) {
    on_commit_triggers_->ExecTriggers();
  }
}

}  // namespace concurrency
}  // namespace peloton
