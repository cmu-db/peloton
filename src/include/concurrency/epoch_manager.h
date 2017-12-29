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

class EpochManager {
  EpochManager(const EpochManager&) = delete;

public:
  EpochManager() {}

  // TODO: stop epoch threads before resetting epoch id.
  virtual void Reset() = 0;

  virtual void Reset(const uint64_t epoch_id) = 0;

  virtual void SetCurrentEpochId(const uint64_t epoch_id) = 0;

  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) = 0;

  virtual void StartEpoch() = 0;

  virtual void StopEpoch() = 0;

  //====================================================
  // designed for decentralized epoch manager
  //====================================================
  virtual void RegisterThread(const size_t thread_id) = 0;

  virtual void DeregisterThread(const size_t thread_id) = 0;

  virtual cid_t EnterEpoch(const size_t thread_id, const TimestampType timestamp_type) = 0;

  virtual void ExitEpoch(const size_t thread_id, const eid_t epoch_id) = 0;

  virtual eid_t GetExpiredEpochId() = 0;

  virtual eid_t GetNextEpochId() = 0;

  virtual eid_t GetCurrentEpochId() = 0;  

  virtual cid_t GetExpiredCid() = 0;

};

}
}

