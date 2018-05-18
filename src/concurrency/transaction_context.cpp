//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.cpp
//
// Identification: src/concurrency/transaction_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_context.h"

#include <sstream>

#include "common/logger.h"
#include "common/macros.h"
#include "common/platform.h"
#include "trigger/trigger.h"

#include <chrono>
#include <iomanip>
#include <thread>

namespace peloton {
namespace concurrency {

/*
 * TransactionContext state transition:
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

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id) {
  Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id) {
  Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::~TransactionContext() {}

void TransactionContext::Init(const size_t thread_id,
                              const IsolationLevelType isolation,
                              const cid_t &read_id, const cid_t &commit_id) {
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

RWType TransactionContext::GetRWType(const ItemPointer &location) {
  RWType rw_type = RWType::INVALID;

  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    return rw_set_it->second;
  }
  return rw_type;
}

void TransactionContext::RecordRead(const ItemPointer &location) {

  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    UNUSED_ATTRIBUTE RWType rw_type = rw_set_it->second;
    PELOTON_ASSERT(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
    return;
  }
  rw_set_[location] = RWType::READ;
}

void TransactionContext::RecordReadOwn(const ItemPointer &location) {
  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    RWType rw_type = rw_set_it->second;
    PELOTON_ASSERT(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
    if (rw_type != RWType::READ) {
      return;
    }
  }
  rw_set_[location] = RWType::READ_OWN;
}

void TransactionContext::RecordUpdate(const ItemPointer &location) {
  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    RWType rw_type = rw_set_it->second;
    if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
      is_written_ = true;
    } else if (rw_type == RWType::UPDATE || rw_type == RWType::INSERT) {
      return;
    } else {
      // DELETE or INS_DELETE
      PELOTON_ASSERT(false);
      return;
    }
  }
  rw_set_[location] = RWType::UPDATE;
}

void TransactionContext::RecordInsert(const ItemPointer &location) {
  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    PELOTON_ASSERT(false);
    return;
  }
  rw_set_[location] = RWType::INSERT;
  ++insert_count_;
}

bool TransactionContext::RecordDelete(const ItemPointer &location) {
  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    RWType rw_type = rw_set_it->second;

    if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
      rw_set_[location] = RWType::DELETE;
      is_written_ = true;
      return false;
    } else if (rw_type == RWType::UPDATE) {
      rw_set_[location] = RWType::DELETE;
      return false;
    } else if (rw_type == RWType::INSERT) {
      rw_set_[location] = RWType::INS_DEL;
      --insert_count_;
      return true;
    } else {
      // DELETE and INS_DEL
      PELOTON_ASSERT(false);
      return false;
    }
  }
  rw_set_[location] = RWType::DELETE;
  return false;
}

const std::string TransactionContext::GetInfo() const {
  std::ostringstream os;

  os << " Txn :: @" << this << " ID : " << std::setw(4) << txn_id_
     << " Read ID : " << std::setw(4) << read_id_
     << " Commit ID : " << std::setw(4) << commit_id_
     << " Result : " << result_;

  return os.str();
}

void TransactionContext::AddOnCommitTrigger(
    trigger::TriggerData &trigger_data) {
  if (on_commit_triggers_ == nullptr) {
    on_commit_triggers_.reset(new trigger::TriggerSet());
  }
  on_commit_triggers_->push_back(trigger_data);
}

void TransactionContext::ExecOnCommitTriggers() {
  if (on_commit_triggers_ != nullptr) {
    on_commit_triggers_->ExecTriggers();
  }
}

}  // namespace concurrency
}  // namespace peloton
