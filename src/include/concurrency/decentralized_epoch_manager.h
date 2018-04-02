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
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/platform.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include "common/synchronization/spin_latch.h"
#include "concurrency/epoch_manager.h"
#include "concurrency/local_epoch.h"

namespace peloton {
namespace concurrency {

/**
 * @brief      Class for decentralized epoch manager.
 */
class DecentralizedEpochManager : public EpochManager {
  DecentralizedEpochManager(const DecentralizedEpochManager&) = delete;

public:
  DecentralizedEpochManager() : 
    current_global_epoch_id_(1), 
    next_txn_id_(0),
    snapshot_global_epoch_id_(1),
    is_running_(false) {
      // register a default thread for handling catalog stuffs.
      RegisterThread(0);
  }

  /**
   * @brief      Gets the instance.
   *
   * @return     The instance.
   */
  static DecentralizedEpochManager &GetInstance() {
    static DecentralizedEpochManager epoch_manager;
    return epoch_manager;
  }

  virtual void Reset() override {
    Reset(1);
  }

  /**
   * @brief      Reset the current global epoch id to the current epoch 
   *             identifier
   *
   * @param[in]  current_epoch_id  The current epoch identifier
   */
  virtual void Reset(const uint64_t current_epoch_id) override {
    // epoch should be always larger than 0
    PELOTON_ASSERT(current_epoch_id != 0);
    current_global_epoch_id_ = current_epoch_id;
    next_txn_id_ = 0;
    snapshot_global_epoch_id_ = 1;
    local_epochs_.clear();
    
    RegisterThread(0);
  }

  /**
   * @brief      Sets the current epoch identifier.
   *
   * @param[in]  current_epoch_id  The current epoch identifier
   */
  virtual void SetCurrentEpochId(const uint64_t current_epoch_id) override {
    current_global_epoch_id_ = current_epoch_id;
    next_txn_id_ = 0;
  }

  /**
   * @brief      Starts an epoch.
   *
   * @param      epoch_thread  The epoch thread
   */
  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    epoch_thread.reset(new std::thread(&DecentralizedEpochManager::Running, this));
  }

  /**
   * @brief      Starts an epoch.
   */
  virtual void StartEpoch() override {
    LOG_TRACE("Starting epoch");
    this->is_running_ = true;
    thread_pool.SubmitDedicatedTask(&DecentralizedEpochManager::Running, this);
  }

  /**
   * @brief      Stops an epoch.
   */
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

  /**
   * @brief      A transaction enters epoch with thread id
   *
   * @param[in]  thread_id  The thread identifier
   * @param[in]  ts_type    The ts type
   *
   * @return     The expired epoch identifier.
   */
  virtual cid_t EnterEpoch(const size_t thread_id, const TimestampType ts_type) override;

  /**
   * @brief      A transaction exits epoch with thread id
   *
   * @param[in]  thread_id  The thread identifier
   * @param[in]  epoch_id   The epoch identifier
   */
  virtual void ExitEpoch(const size_t thread_id, const eid_t epoch_id) override;


  /**
   * @brief      Gets the expired cid.
   *
   * @return     The expired cid.
   */
  virtual cid_t GetExpiredCid() override {
    uint64_t max_committed_eid = GetExpiredEpochId();
    return (max_committed_eid << 32) | 0xFFFFFFFF;
  }

  /**
   * @brief      Gets the expired epoch identifier.
   *
   * @return     The expired epoch identifier.
   */
  virtual eid_t GetExpiredEpochId() override;

  /**
   * @brief      Gets the next epoch identifier.
   *
   * @return     The next epoch identifier.
   */
  virtual eid_t GetNextEpochId() override {
    return current_global_epoch_id_ + 1;
  }

  /**
   * @brief      Gets the current epoch identifier.
   *
   * @return     The current epoch identifier.
   */
  virtual eid_t GetCurrentEpochId() override {
    return current_global_epoch_id_.load();
  }  

private:


  /**
   * @brief      Gets the next transaction identifier.
   *
   * @return     The next transaction identifier.
   */
  inline uint32_t GetNextTransactionId() {
    return next_txn_id_.fetch_add(1, std::memory_order_relaxed);
  }


  void Running() {

    PELOTON_ASSERT(is_running_ == true);

    while (is_running_ == true) {
      // the epoch advances every EPOCH_LENGTH milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));
      current_global_epoch_id_.fetch_add(1);
    }
  }

private:

  /**
   * Each thread holds a pointer to a local epoch.
   * It updates the local epoch to report their local time.
   */
  common::synchronization::SpinLatch local_epoch_lock_;
  std::unordered_map<int, std::unique_ptr<LocalEpoch>> local_epochs_;
  
  /** The global epoch reflects the true time of the system. */
  std::atomic<eid_t> current_global_epoch_id_;
  std::atomic<uint32_t> next_txn_id_;
  
  /**
   * Snapshot epoch is an epoch where the corresponding tuples may be still
   * visible to on-the-fly transactions
   */
  eid_t snapshot_global_epoch_id_;

  bool is_running_;

};

}
}

