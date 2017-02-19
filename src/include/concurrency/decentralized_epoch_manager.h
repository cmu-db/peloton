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

namespace peloton {
namespace concurrency {

class LocalEpochContext {

struct Epoch {
  std::atomic<size_t> read_only_count_;
  std::atomic<size_t> read_write_count_;

  Epoch(): 
    read_only_count_(0), 
    read_write_count_(0) {}

  Epoch(const Epoch &epoch): 
    read_only_count_(epoch.read_only_count_.load()), 
    read_write_count_(epoch.read_write_count_.load()) {}

  void Init() {
    read_only_count_ = 0;
    read_write_count_ = 0;
  }
};

public:
  LocalEpochContext() : current_epoch_(0) {}

  void EnterEpoch() {}

  void ExitEpoch() {}

private:
  size_t current_epoch_;

  std::vector<Epoch> epoch_buffer_;
};

class DecentralizedEpochManager : public EpochManager {
  DecentralizedEpochManager(const DecentralizedEpochManager&) = delete;

public:
  DecentralizedEpochManager() : 
    local_epoch_context_id_(0), 
    current_global_epoch_(0), 
    current_transaction_id_(0) {}

  virtual void Reset(const size_t &current_epoch) override {
    current_global_epoch_ = current_epoch;
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

  size_t RegisterLocalEpochContext(const size_t epoch_context_id) {
    local_epoch_context_lock_.Lock();

    // push back to context vector.
    local_epoch_contexts_[epoch_context_id].reset(new LocalEpochContext());
    local_epoch_context_id_++;

    local_epoch_context_lock_.Unlock();

    return local_epoch_context_id_;
  }


  // enter epoch with epoch context id (essentially an identifier of the corresponding thread)
  cid_t EnterEpoch(const size_t epoch_context_id, const size_t epoch_id) {
    uint32_t epoch_id = GetCurrentGlobalEpoch();
    uint32_t transaction_id = GetCurrentTransactionId();

    PL_ASSERT(local_epoch_contexts_.find(epoch_context_id) != local_epoch_contexts_.end());

    // enter the corresponding local epoch.
    local_epoch_contexts_.at(epoch_context_id)->ExitEpoch();

    return (epoch_id << 32) | transaction_id;
  }

  void ExitEpoch(const size_t epoch_context_id) {
    
  }

  cid_t GetMaxDeadEpochId() {
    uint32_t min_epoch_id = std::numeric_limits<uint32_t>::max();
    for (size_t i = 0; i < local_epoch_context_id_; ++i) {
      min_epoch_id = std::min(local_epoch_contexts_[i]->current_epoch_, min_epoch_id);
    }
    return min_epoch_id;
  }

private:

  inline uint32_t GetCurrentGlobalEpoch() {
    return current_global_epoch_.load();
  }

  inline uint32_t GetCurrentTransactionId() {
    return current_transaction_id_.fetch_add(1, std::memory_order_relaxed);
  }


  void Running() {

    PL_ASSERT(is_running_ == true);

    while (is_running_ == true) {
      // the epoch advances every EPOCH_LENGTH milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));

    }
  }

private:
    
  // each (logical) thread holds a pointer to a local epoch context.
  // it updates the local epoch context to report their local time.
  Spinlock local_epoch_context_lock_;
  std::unordered_map<int, std::unique_ptr<LocalEpochContext>> local_epoch_contexts_;
  size_t local_epoch_context_id_;
  
  // the global epoch reflects the true time of the system.
  std::atomic<uint32_t> current_global_epoch_;
  std::atomic<uint32_t> current_transaction_id_;
  
  bool is_running_;

};

}
}

