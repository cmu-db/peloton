//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager.h
//
// Identification: src/include/concurrency/centralized_epoch_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <vector>

#include "common/macros.h"
#include "type/types.h"
#include "common/logger.h"
#include "common/platform.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include "concurrency/epoch_manager.h"

namespace peloton {
namespace concurrency {

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

class CentralizedEpochManager : public EpochManager {
  CentralizedEpochManager(const CentralizedEpochManager&) = delete;
  static const int safety_interval_ = 2;

struct Epoch {
  std::atomic<int> ro_txn_ref_count_;
  std::atomic<int> rw_txn_ref_count_;
  cid_t max_cid_;

  Epoch(): 
    ro_txn_ref_count_(0), 
    rw_txn_ref_count_(0),
    max_cid_(0) {}

  Epoch(const Epoch &epoch): 
    ro_txn_ref_count_(epoch.ro_txn_ref_count_.load()), 
    rw_txn_ref_count_(epoch.rw_txn_ref_count_.load()),
    max_cid_(0) {}

  void Init() {
    ro_txn_ref_count_ = 0;
    rw_txn_ref_count_ = 0;
    max_cid_ = 0;
  }
};

public:
  CentralizedEpochManager()
    : epoch_queue_(epoch_queue_size_),
      queue_tail_(0), 
      reclaim_tail_(0), 
      current_epoch_(0),
      queue_tail_token_(true), 
      reclaim_tail_token_(true),
      max_cid_ro_(READ_ONLY_START_CID), 
      max_cid_gc_(0), 
      is_running_(false) {}


  static CentralizedEpochManager &GetInstance() {
    static CentralizedEpochManager epoch_manager;
    return epoch_manager;
  }

  virtual void Reset(const size_t &current_epoch) override {
    current_epoch_ = current_epoch;
  }

  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    epoch_thread.reset(new std::thread(&CentralizedEpochManager::Running, this));

  }

  virtual void StartEpoch() override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    thread_pool.SubmitDedicatedTask(&CentralizedEpochManager::Running, this);
  }

  virtual void StopEpoch() override {
    LOG_TRACE("Stopping epoch");
    this->is_running_ = false;
  }

  virtual size_t EnterReadOnlyEpoch(cid_t begin_cid) override {
    auto epoch = queue_tail_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_++;

    // Set the max cid in the tuple
    auto max_cid_ptr = &(epoch_queue_[epoch_idx].max_cid_);
    AtomicMax(max_cid_ptr, begin_cid);

    return epoch;
  }

  virtual size_t EnterEpoch(cid_t begin_cid) override {
    auto epoch = current_epoch_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_++;

    // Set the max cid in the tuple
    auto max_cid_ptr = &(epoch_queue_[epoch_idx].max_cid_);
    AtomicMax(max_cid_ptr, begin_cid);

    return epoch;
  }

  virtual void ExitReadOnlyEpoch(size_t epoch) override {
    PL_ASSERT(epoch >= reclaim_tail_);
    PL_ASSERT(epoch <= queue_tail_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_--;
  }

  virtual void ExitEpoch(size_t epoch) override {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_--;
  }

  // assume we store epoch_store max_store previously
  virtual cid_t GetMaxCommittedCid() override {
    IncreaseQueueTail();
    IncreaseReclaimTail();

    return max_cid_gc_;
  }

  virtual cid_t GetReadOnlyTxnCid() override {
    IncreaseQueueTail();
    return max_cid_ro_;
  }

private:
  void Running();

  void IncreaseReclaimTail();

  void IncreaseQueueTail();

  void AtomicMax(cid_t* addr, cid_t max) {
    while(true) {
      auto old = *addr;
      if(old > max) {
        return;
      }else if ( __sync_bool_compare_and_swap(addr, old, max) ) {
        return;
      }
    }
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
  cid_t max_cid_ro_;
  cid_t max_cid_gc_;
  bool is_running_;
};


}
}

