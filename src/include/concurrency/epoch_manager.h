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

namespace peloton {
namespace concurrency {

class EpochManager {
  EpochManager(const EpochManager&) = delete;

public:
  EpochManager() {}

  // TODO: stop epoch threads before resetting epoch id.
  virtual void Reset(const size_t &current_epoch) = 0;

  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) = 0;

  virtual void StartEpoch() = 0;

  virtual void StopEpoch() = 0;

  //====================================================
  // designed for decentralized epoch manager
  //====================================================
  virtual void RegisterThread(const size_t thread_id) = 0;

  virtual void DeregisterThread(const size_t thread_id) = 0;

  virtual cid_t EnterEpoch(const size_t thread_id, const bool is_snapshot_read) = 0;

  virtual void ExitEpoch(const size_t thread_id, const cid_t begin_cid) = 0;

  virtual cid_t GetMaxCommittedCid() = 0;

  virtual uint64_t GetMaxCommittedEpochId() = 0;

};

}
}

