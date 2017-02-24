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

#include "concurrency/centralized_epoch_manager.h"


namespace peloton {
namespace concurrency {
  void CentralizedEpochManager::Running() {
    
    PL_ASSERT(is_running_ == true);
    
    while (is_running_ == true) {
      // the epoch advances every EPOCH_LENGTH milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));

      auto next_idx = (current_epoch_.load() + 1) % epoch_queue_size_;
      auto tail_idx = reclaim_tail_.load() % epoch_queue_size_;

      if(next_idx == tail_idx) {
        // overflow
        // in this case, just increase tail
        IncreaseQueueTail();
        IncreaseReclaimTail();
        continue;
      }

      // we have to init it first, then increase current epoch
      // otherwise may read dirty data
      epoch_queue_[next_idx].Init();
      current_epoch_++;

      IncreaseQueueTail();
      IncreaseReclaimTail();
    }
  }

  void CentralizedEpochManager::IncreaseReclaimTail() {
    bool expect = true, desired = false;
    if(!reclaim_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = queue_tail_.load();
    auto tail = reclaim_tail_.load();

    while(true) {
      if(tail + safety_interval_ >= current) {
        break;
      }

      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].ro_txn_ref_count_ > 0) {
        break;
      }

      // save max cid
      auto max = epoch_queue_[idx].max_cid_;
      AtomicMax(&max_cid_gc_, max);
      tail++;
    }

    reclaim_tail_ = tail;

    expect = false;
    desired = true;

    reclaim_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

  void CentralizedEpochManager::IncreaseQueueTail() {
    bool expect = true, desired = false;
    if(!queue_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = current_epoch_.load();
    auto tail = queue_tail_.load();

    while(true) {
      if(tail + safety_interval_ >= current) {
        break;
      }

      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].rw_txn_ref_count_ > 0) {
        break;
      }

      // save max cid
      auto max = epoch_queue_[idx].max_cid_;
      AtomicMax(&max_cid_ro_, max);
      tail++;
    }

    queue_tail_ = tail;

    expect = false;
    desired = true;

    queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }


}
}