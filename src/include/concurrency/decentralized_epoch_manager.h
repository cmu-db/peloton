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


class DecentralizedEpochManager : public EpochManager {
  DecentralizedEpochManager(const DecentralizedEpochManager&) = delete;

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

struct LocalEpochContext {

  LocalEpochContext() : 
    epoch_buffer_(epoch_buffer_size_),
    init_epoch_(UINT64_MAX),
    max_dead_epoch_(UINT64_MAX) {}

  void EnterLocalEpoch(const uint64_t epoch_id) {
    // if not initiated
    if (init_epoch_ == UINT64_MAX) {
      init_epoch_ = epoch_id - epoch_id % epoch_buffer_size_;
    } 
    
    PL_ASSERT(epoch_id - init_epoch_ < epoch_buffer_size_);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx].read_write_count_++;
  }

  void EnterLocalReadOnlyEpoch(const uint64_t epoch_id) {
    // if not initiated
    if (init_epoch_ == UINT64_MAX) {
      init_epoch_ = epoch_id - epoch_id % epoch_buffer_size_;
    } 
    
    PL_ASSERT(epoch_id - init_epoch_ < epoch_buffer_size_);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx].read_only_count_++;
  }

  void ExitLocalEpoch(const uint64_t epoch_id) {
    PL_ASSERT(init_epoch_ != UINT64_MAX);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx].read_write_count_--;

  }

  void ExitLocalReadOnlyEpoch(const uint64_t epoch_id) {
    PL_ASSERT(init_epoch_ != UINT64_MAX);

    size_t epoch_idx = (size_t) epoch_id % epoch_buffer_size_;
    epoch_buffer_[epoch_idx].read_only_count_--;
  }

  // queue size
  static const size_t epoch_buffer_size_ = 4096;
  std::vector<Epoch> epoch_buffer_;

  uint64_t init_epoch_;
  uint64_t max_dead_epoch_;
};

public:
  DecentralizedEpochManager() : 
    current_global_epoch_(0), 
    next_txn_id_(0),
    is_running_(false) {}


  static DecentralizedEpochManager &GetInstance() {
    static DecentralizedEpochManager epoch_manager;
    return epoch_manager;
  }

  virtual void Reset(const size_t &current_epoch) override {
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

  virtual void RegisterLocalEpochContext(const size_t thread_id) override {
    local_epoch_context_lock_.Lock();

    local_epoch_contexts_[thread_id].reset(new LocalEpochContext());

    local_epoch_context_lock_.Unlock();
  }

  virtual void DeregisterLocalEpochContext(const size_t thread_id) override {
    local_epoch_context_lock_.Lock();

    local_epoch_contexts_.erase(thread_id);

    local_epoch_context_lock_.Unlock();
  }

  // enter epoch with thread id
  virtual cid_t EnterEpochD(const size_t thread_id) override {

    uint64_t epoch_id = GetCurrentGlobalEpoch();
    uint32_t next_txn_id = GetNextTransactionId();

    PL_ASSERT(local_epoch_contexts_.find(thread_id) != local_epoch_contexts_.end());

    // enter the corresponding local epoch.
    local_epoch_contexts_.at(thread_id)->EnterLocalEpoch(epoch_id);

    return (epoch_id << 32) | next_txn_id;
  }

  virtual void ExitEpochD(const size_t thread_id, const size_t begin_cid) override {

    uint64_t epoch_id = ExtractEpochId(begin_cid);

    PL_ASSERT(local_epoch_contexts_.find(thread_id) != local_epoch_contexts_.end());

    // enter the corresponding local epoch.
    local_epoch_contexts_.at(thread_id)->ExitLocalEpoch(epoch_id);
 
  }


  cid_t GetMaxDeadEpochId() {
    uint32_t min_epoch_id = std::numeric_limits<uint32_t>::max();
    
    for (auto &local_epoch_context : local_epoch_contexts_) {
      if (local_epoch_context.second->max_dead_epoch_ < min_epoch_id) {
        min_epoch_id = local_epoch_context.second->max_dead_epoch_;
      }
    }

    return min_epoch_id;
  }

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

    }
  }

private:

  // each thread holds a pointer to a local epoch context.
  // it updates the local epoch context to report their local time.
  Spinlock local_epoch_context_lock_;
  std::unordered_map<int, std::unique_ptr<LocalEpochContext>> local_epoch_contexts_;
  
  // the global epoch reflects the true time of the system.
  std::atomic<uint64_t> current_global_epoch_;
  std::atomic<uint32_t> next_txn_id_;
  
  bool is_running_;

};

}
}

