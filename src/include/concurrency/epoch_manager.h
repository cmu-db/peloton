
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

#include "type/types.h"
#include "common/macros.h"
#include "common/logger.h"
#include "common/platform.h"

namespace peloton {
namespace concurrency {

class EpochManager {
  EpochManager(const EpochManager&) = delete;

public:
  EpochManager() {}

  static inline size_t GetEpochQueueCapacity() {return 40960;}

  size_t GetEpochLengthInMicroSecQuarter() const {
      return (int)(1000 * 1000 / 4); //epoch_duration_millisec_
    }
  size_t GetEpochDurationMilliSecond() const {
      return (int)(1000); //epoch_duration_millisec_
    }

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
