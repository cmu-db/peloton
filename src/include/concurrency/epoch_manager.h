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

#include "common/internal_types.h"

namespace peloton {
namespace concurrency {

/**
 * @brief      Class for epoch manager.
 */
class EpochManager {
  EpochManager(const EpochManager&) = delete;

public:
  EpochManager() {}

  /** TODO: stop epoch threads before resetting epoch id. */
  virtual void Reset() = 0;

  /**
   * @brief      Reset epoch_id
   *
   * @param[in]  epoch_id  The epoch identifier
   */
  virtual void Reset(const uint64_t epoch_id) = 0;

  /**
   * @brief      Sets the current epoch identifier.
   *
   * @param[in]  epoch_id  The epoch identifier
   */
  virtual void SetCurrentEpochId(const uint64_t epoch_id) = 0;

  /**
   * @brief      Starts an epoch.
   *
   * @param      epoch_thread  The epoch thread
   */
  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) = 0;

  /**
   * @brief      Starts an epoch.
   */
  virtual void StartEpoch() = 0;

  /**
   * @brief      Stops an epoch.
   */
  virtual void StopEpoch() = 0;

  //====================================================
  // designed for decentralized epoch manager
  //====================================================

  virtual void RegisterThread(const size_t thread_id) = 0;

  virtual void DeregisterThread(const size_t thread_id) = 0;

  virtual cid_t EnterEpoch(const size_t thread_id, const TimestampType timestamp_type) = 0;

  virtual void ExitEpoch(const size_t thread_id, const eid_t epoch_id) = 0;

  /**
   * @brief      Gets the expired epoch identifier.
   *
   * @return     The expired epoch identifier.
   */
  virtual eid_t GetExpiredEpochId() = 0;

  /**
   * @brief      Gets the next epoch identifier.
   *
   * @return     The next epoch identifier.
   */
  virtual eid_t GetNextEpochId() = 0;

  /**
   * @brief      Gets the current epoch identifier.
   *
   * @return     The current epoch identifier.
   */
  virtual eid_t GetCurrentEpochId() = 0;  

  /**
   * @brief      Gets the expired cid.
   *
   * @return     The expired cid.
   */
  virtual cid_t GetExpiredCid() = 0;

};

}
}

