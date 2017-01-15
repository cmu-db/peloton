//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager.h
//
// Identification: src/include/concurrency/epoch_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <vector>

#include "common/macros.h"
#include "type/types.h"
#include "common/platform.h"
#include "common/init.h"
#include "common/thread_pool.h"

namespace peloton {
namespace concurrency {

struct Epoch {
  std::atomic<int> ro_txn_ref_count_;
  std::atomic<int> rw_txn_ref_count_;

  Epoch(): 
    ro_txn_ref_count_(0), 
    rw_txn_ref_count_(0) {}

  Epoch(const Epoch &epoch): 
    ro_txn_ref_count_(epoch.ro_txn_ref_count_.load()), 
    rw_txn_ref_count_(epoch.rw_txn_ref_count_.load()) {}

  void Init() {
    ro_txn_ref_count_ = 0;
    rw_txn_ref_count_ = 0;
  }
};

/*
Epoch queue layout:
 current epoch               queue tail                reclaim tail
/                           /                          /
+--------+--------+--------+--------+--------+--------+--------+-------
| head   | safety |  ....  |readonly| safety |  ....  |gc usage|  ....
+--------+--------+--------+--------+--------+--------+--------+-------
New                                                   Old

Note:
1) Queue tail epoch and epochs which is older than it have 0 rw txn ref count
2) Reclaim tail epoch and epochs which is older than it have 0 ro txn ref count
3) Reclaim tail is at least 2 turns older than the queue tail epoch
4) Queue tail is at least 2 turns older than the head epoch
*/

class EpochManager {
  EpochManager(const EpochManager&) = delete;
  static const int safety_interval_ = 2;

public:
  EpochManager()
    : epoch_queue_(epoch_queue_size_),
      queue_tail_(RO_BEGIN_EPOCH),
      reclaim_tail_(MIN_EPOCH),
      current_epoch_(RW_BEGIN_EPOCH),
      queue_tail_token_(true), 
      reclaim_tail_token_(true),
      finish_(false) {
  }

  void Reset(const size_t &current_epoch) {
    current_epoch_ = current_epoch;
  }

  void StartEpoch() {
    finish_ = false;
    thread_pool.SubmitDedicatedTask(&EpochManager::Start, this);
  }

  void StopEpoch() {
    finish_ = true;
  }

  static inline size_t ExtractEpochIdFromCid(cid_t cid) {
    return (cid >> 32) & 0xffffffff;
  }

  cid_t EnterReadOnlyEpoch() {
    auto epoch = queue_tail_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    size_t cnt = epoch_queue_[epoch_idx].ro_txn_ref_count_++;

    // Validation
    auto validate_epoch = queue_tail_.load();
    while (validate_epoch != epoch) {
      // Reset the registered epoch
      epoch_queue_[epoch_idx].ro_txn_ref_count_--;
      // Register into new current epoch
      epoch = validate_epoch;
      epoch_idx = epoch % epoch_queue_size_;
      cnt = epoch_queue_[epoch_idx].ro_txn_ref_count_++;
      // Get new validate epoch
      validate_epoch = queue_tail_.load();
    }

    return MakeCid(epoch, cnt);
  }

  cid_t EnterEpoch() {
    auto epoch = current_epoch_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    size_t cnt = epoch_queue_[epoch_idx].rw_txn_ref_count_++;

    // Validation
    auto validate_epoch = current_epoch_.load();
    while (validate_epoch != epoch) {
      // Reset the registered epoch
      epoch_queue_[epoch_idx].rw_txn_ref_count_--;
      // Register into new current epoch
      epoch = validate_epoch;
      epoch_idx = epoch % epoch_queue_size_;
      cnt = epoch_queue_[epoch_idx].rw_txn_ref_count_++;
      // Get new validate epoch
      validate_epoch = current_epoch_.load();
    }

    return MakeCid(epoch, cnt);
  }

  cid_t GetNextSafeCid() {
    return MakeCid(current_epoch_.load(), 0xffffffff);
  }

  void ExitReadOnlyEpoch(size_t epoch) {
    PL_ASSERT(epoch >= reclaim_tail_);
    PL_ASSERT(epoch <= queue_tail_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_--;
  }

  void ExitEpoch(size_t epoch) {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_--;
  }

  // assume we store epoch_store max_store previously
  cid_t GetMaxDeadTxnCid() {
    IncreaseQueueTail();
    IncreaseReclaimTail();

    return MakeCid(reclaim_tail_.load(), 0);
  }

private:
  inline cid_t MakeCid(size_t eid, size_t cnt) {
    uint64_t high32 = (eid & 0xffffffff) << 32;
    return high32 | (cnt & 0xffffffff);
  }

  void Start() {
    while (!finish_) {
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

  void IncreaseReclaimTail() {
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

      tail++;
    }

    reclaim_tail_ = tail;

    expect = false;
    desired = true;

    reclaim_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

  void IncreaseQueueTail() {
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
      tail++;
    }

    queue_tail_ = tail;

    expect = false;
    desired = true;

    queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

  inline void InitEpochQueue() {
    for (int i = 0; i < 5; ++i) {
      epoch_queue_[i].Init();
    }

    current_epoch_ = 0;
    queue_tail_ = 0;
    reclaim_tail_ = 0;
  }

private:
  // queue size
  static const size_t epoch_queue_size_ = 4096;

  // Epoch vector
  std::vector<Epoch> epoch_queue_;
  std::atomic<size_t> queue_tail_;
  std::atomic<size_t> reclaim_tail_;
  std::atomic<size_t> current_epoch_;
  std::atomic<bool> queue_tail_token_;
  std::atomic<bool> reclaim_tail_token_;
  bool finish_;
};


}
}

