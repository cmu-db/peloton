//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager.cpp
//
// Identification: src/concurrency/centralized_epoch_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/decentralized_epoch_manager.h"


namespace peloton {
namespace concurrency {

  bool LocalEpoch::EnterEpoch(const uint64_t epoch_id) {    
    // ***critical sections for updating tail_epoch_id.
    tail_lock_.Lock();
    // if not initiated
    if (tail_epoch_id_ == UINT64_MAX) {
      tail_epoch_id_ = epoch_id - 1;
    }
    tail_lock_.Unlock();
    // ************************************************

    // ideally, epoch_id should never be smaller than head_epoch id.
    // however, as we force updating head_epoch_id in GetTailEpochId() function,
    // it is possible that we find that epoch_id is smaller than head_epoch_id.
    // in this case, we should reject entering local epoch.
    // this is essentially a validation scheme.

    // ***critical sections for updating head_epoch_id.
    head_lock_.Lock();
    if (epoch_id < head_epoch_id_) {
      head_lock_.Unlock();
      return false;
    } else {
      PL_ASSERT(epoch_id >= head_epoch_id_);
      
      // insert to epoch buffer prior to releasing lock.
      PL_ASSERT(epoch_id - tail_epoch_id_ <= epoch_buffer_size_);

      size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
      epoch_buffer_[epoch_idx]++;

      // update head epoch id.
      head_epoch_id_ = epoch_id;
      head_lock_.Unlock();
      return true;
    }
    // ************************************************
  }

  void LocalEpoch::ExitEpoch(const uint64_t epoch_id) {
    PL_ASSERT(tail_epoch_id_ != UINT64_MAX);

    PL_ASSERT(epoch_id > tail_epoch_id_);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx]--;

    // when exiting a local epoch, we must check whether it can be reclaimed.
    IncreaseTailEpochId();
  }

  
  uint64_t LocalEpoch::EnterEpochReadOnly(const uint64_t epoch_id) {
    // ***critical sections for updating tail_epoch_id.
    tail_lock_.Lock();
    // if not initiated
    if (tail_epoch_id_ == UINT64_MAX) {
      tail_epoch_id_ = epoch_id - 1;
    } 
    tail_lock_.Unlock();
    // ************************************************

    // ideally, epoch_id should never be smaller than head_epoch id.
    // however, as we force updating head_epoch_id in GetTailEpochId() function,
    // it is possible that we find that epoch_id is smaller than head_epoch_id.
    // in this case, we should reject entering local epoch.
    // this is essentially a validation scheme.

    // ***critical sections for updating head_epoch_id.
    head_lock_.Lock();
    if (epoch_id < head_epoch_id_) {
      head_lock_.Unlock();
      return false;
    } else {
      PL_ASSERT(epoch_id >= head_epoch_id_);
      
      // insert to epoch buffer prior to releasing lock.
      PL_ASSERT(epoch_id - tail_epoch_id_ <= epoch_buffer_size_);

      size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
      epoch_buffer_[epoch_idx]++;

      // update head epoch id.
      head_epoch_id_ = epoch_id;
      head_lock_.Unlock();
      return true;
    }
    // ************************************************
  }

  // for now, we do not support read-only transactions
  void LocalEpoch::ExitEpochReadOnly(const uint64_t epoch_id) {
    PL_ASSERT(tail_epoch_id_ != UINT64_MAX);

    PL_ASSERT(epoch_id > tail_epoch_id_);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx]--;

    // when exiting a local epoch, we must check whether it can be reclaimed.
    IncreaseTailEpochId();
  }

  // this function can be invoked by both the epoch thread and the local worker thread.
  void LocalEpoch::IncreaseTailEpochId() {
    tail_lock_.Lock();
    // in the best case, tail_epoch_id can be increased to (head_epoch_id - 1)
    while (tail_epoch_id_ < head_epoch_id_ - 1) {
      size_t epoch_idx = (size_t) (tail_epoch_id_ + 1) % epoch_buffer_size_;
      if (epoch_buffer_[epoch_idx] == 0) {
        tail_epoch_id_++;
      } else {
        break;
      }
    }
    tail_lock_.Unlock();
  }

  // this function is periodically invoked.
  // the centralized epoch thread must check the status of each local epoch.
  // the centralized epoch thread should also tell each local epoch the latest time.
  uint64_t LocalEpoch::GetTailEpochId(const uint64_t current_epoch_id) {
    // ***critical sections for updating head_epoch_id.
    head_lock_.Lock();
    if (current_epoch_id > head_epoch_id_) {
      head_epoch_id_ = current_epoch_id;
    } 
    head_lock_.Unlock();
    // ************************************************

    // ***critical sections for updating tail_epoch_id.
    tail_lock_.Lock();
    // this thread never started executing transactions.
    if (tail_epoch_id_ == UINT64_MAX) {
      // force updating tail epoch id to head_epoch_id - 1.
      // it is ok if this operation fails.
      // if fails, then if means that the local thread started a new transaction.
      tail_epoch_id_ = head_epoch_id_ - 1;
    } else {
      // in the best case, tail_epoch_id can be increased to (head_epoch_id - 1)
      while (tail_epoch_id_ < head_epoch_id_ - 1) {
        size_t epoch_idx = (size_t) (tail_epoch_id_ + 1) % epoch_buffer_size_;
        if (epoch_buffer_[epoch_idx] == 0) {
          tail_epoch_id_++;
        } else {
          break;
        }
      }
    }
    tail_lock_.Unlock();
    // ************************************************

    return tail_epoch_id_;
  }

}
}