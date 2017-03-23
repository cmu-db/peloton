//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decentralized_epoch_manager.h
//
// Identification: src/include/concurrency/decentralized_epoch_manager.h
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
#include "concurrency/local_epoch.h"

namespace peloton {
namespace concurrency {

class DecentralizedEpochManager : public EpochManager {
  DecentralizedEpochManager(const DecentralizedEpochManager&) = delete;

public:
  DecentralizedEpochManager() : 
    current_global_epoch_(1), 
    next_txn_id_(0),
    current_global_epoch_snapshot_(1),
    is_running_(false) {
      // register a default thread for handling catalog stuffs.
      RegisterThread(0);
  }

  static DecentralizedEpochManager &GetInstance() {
    static DecentralizedEpochManager epoch_manager;
    return epoch_manager;
  }

  virtual void Reset(const size_t &current_epoch) override {
    // epoch should be always larger than 0
    PL_ASSERT(current_epoch != 0);
    current_global_epoch_ = (uint64_t) current_epoch;
  }

  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    epoch_thread.reset(new std::thread(&DecentralizedEpochManager::Running, this));
  }

  virtual void StartEpoch() override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    thread_pool.SubmitDedicatedTask(&DecentralizedEpochManager::Running, this);
  }

  virtual void StopEpoch() override {
    LOG_TRACE("Stopping epoch");
    this->is_running_ = false;
  }

  virtual void RegisterThread(const size_t thread_id) override {
    local_epoch_lock_.Lock();

    local_epochs_[thread_id].reset(new LocalEpoch(thread_id));

    local_epoch_lock_.Unlock();
  }

  virtual void DeregisterThread(const size_t thread_id) override {
    local_epoch_lock_.Lock();

    local_epochs_.erase(thread_id);

    local_epoch_lock_.Unlock();
  }

  // a transaction enters epoch with thread id
  virtual cid_t EnterEpoch(const size_t thread_id, const bool is_snapshot_read) override;

  virtual void ExitEpoch(const size_t thread_id, const cid_t begin_cid) override;


  virtual cid_t GetMaxCommittedCid() override {
    uint64_t max_committed_eid = GetMaxCommittedEpochId();
    return (max_committed_eid << 32) | 0xFFFFFFFF;
  }

  virtual uint64_t GetMaxCommittedEpochId() override;

private:

  inline uint64_t ExtractEpochId(const cid_t cid) {
    return (cid >> 32);
  }

  inline uint64_t GetCurrentGlobalEpoch() {
    return current_global_epoch_.load();
  }

  inline uint32_t GetNextTransactionId() {
    return next_txn_id_.fetch_add(1, std::memory_order_relaxed);
  }


  void Running() {

    PL_ASSERT(is_running_ == true);

    while (is_running_ == true) {
      // the epoch advances every EPOCH_LENGTH milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));
      current_global_epoch_.fetch_add(1);
    }
  }

private:

  // each thread holds a pointer to a local epoch.
  // it updates the local epoch to report their local time.
  Spinlock local_epoch_lock_;
  std::unordered_map<int, std::unique_ptr<LocalEpoch>> local_epochs_;
  
  // the global epoch reflects the true time of the system.
  std::atomic<uint64_t> current_global_epoch_;
  std::atomic<uint32_t> next_txn_id_;
  
  uint64_t current_global_epoch_snapshot_;

  bool is_running_;

};

}
}

