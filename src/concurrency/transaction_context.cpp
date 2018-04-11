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
#include "common/platform.h"
#include "common/macros.h"
#include "trigger/trigger.h"

#include <chrono>
#include <thread>
#include <iomanip>

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
                                       const cid_t &read_id)
    : rw_set_(INTITIAL_RW_SET_SIZE) {
  Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id)
    : rw_set_(INTITIAL_RW_SET_SIZE) {
  Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id,
                                       const size_t rw_set_size)
    : rw_set_(rw_set_size) {
  Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::~TransactionContext() {}

void TransactionContext::Init(const size_t thread_id,
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

  rw_set_.Clear();
  gc_set_.reset(new GCSet());
  gc_object_set_.reset(new GCObjectSet());

  on_commit_triggers_.reset();
}

RWType TransactionContext::GetRWType(const ItemPointer &location) {
  RWType rw_type;

  if (!rw_set_.Find(location, rw_type)) {
    rw_type = RWType::INVALID;
  }
  return rw_type;
}

void TransactionContext::RecordRead(const ItemPointer &location) {
  RWType rw_type;

  if (rw_set_.Find(location, rw_type)) {
    PELOTON_ASSERT(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
    return;
  } else {
    rw_set_.Insert(location, RWType::READ);
  }
}

void TransactionContext::RecordReadOwn(const ItemPointer &location) {
  RWType rw_type;

  if (rw_set_.Find(location, rw_type)) {
    PELOTON_ASSERT(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
    if (rw_type == RWType::READ) {
      rw_set_.Update(location, RWType::READ_OWN);
    } 
  } else {
    rw_set_.Insert(location, RWType::READ_OWN);
  }
}

void TransactionContext::RecordUpdate(const ItemPointer &location) {
  RWType rw_type;

  if (rw_set_.Find(location, rw_type)) {
    if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
      is_written_ = true;
      rw_set_.Update(location, RWType::UPDATE);
      return;
    }
    if (rw_type == RWType::UPDATE) {
      return;
    }
    if (rw_type == RWType::INSERT) {
      return;
    }
    if (rw_type == RWType::DELETE) {
      PELOTON_ASSERT(false);
      return;
    }
    PELOTON_ASSERT(false);
  } else {
    rw_set_.Insert(location, RWType::UPDATE);
  }
}

void TransactionContext::RecordInsert(const ItemPointer &location) {
  if (IsInRWSet(location)) {
    PELOTON_ASSERT(false);
  } else {
    rw_set_.Insert(location, RWType::INSERT);
    ++insert_count_;
  }
}

bool TransactionContext::RecordDelete(const ItemPointer &location) {
  RWType rw_type;

  if (rw_set_.Find(location, rw_type)) {
    if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
      rw_set_.Update(location, RWType::DELETE);
      // record write
      is_written_ = true;
      return false;
    }
    if (rw_type == RWType::UPDATE) {
      rw_set_.Update(location, RWType::DELETE);
      return false;
    }
    if (rw_type == RWType::INSERT) {
      rw_set_.Update(location, RWType::INS_DEL);
      --insert_count_;
      return true;
    }
    if(rw_type == RWType::DELETE) {
      PELOTON_ASSERT(false);
      return false;
    }
    PELOTON_ASSERT(false);
  } else {
    rw_set_.Insert(location, RWType::DELETE);
  }
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

void TransactionContext::AddOnCommitTrigger(trigger::TriggerData &trigger_data) {
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
