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

  virtual void Reset(const size_t &current_epoch) = 0;

  virtual void StartEpoch(std::unique_ptr<std::thread> &epoch_thread) = 0;

  virtual void StartEpoch() = 0;

  virtual void StopEpoch() = 0;

  //====================================================
  // designed for decentralized epoch manager
  //====================================================
  virtual void RegisterThread(const size_t thread_id UNUSED_ATTRIBUTE) { }

  virtual void DeregisterThread(const size_t thread_id UNUSED_ATTRIBUTE) { }

  virtual cid_t EnterEpochD(const size_t thread_id UNUSED_ATTRIBUTE) { return 0; }

  virtual cid_t EnterReadOnlyEpochD(const size_t thread_id UNUSED_ATTRIBUTE) { return 0; }

  virtual void ExitEpochD(const size_t thread_id UNUSED_ATTRIBUTE, const cid_t begin_cid UNUSED_ATTRIBUTE) { }


  virtual uint64_t GetTailEpochId() { return 0; }
  
  //====================================================
  // designed for centralized epoch manager
  //====================================================
  virtual size_t EnterReadOnlyEpoch(cid_t begin_cid UNUSED_ATTRIBUTE) { return 0; }

  virtual size_t EnterEpoch(cid_t begin_cid UNUSED_ATTRIBUTE) { return 0; }

  virtual void ExitReadOnlyEpoch(size_t epoch UNUSED_ATTRIBUTE) {}

  virtual void ExitEpoch(size_t epoch UNUSED_ATTRIBUTE) {}

  virtual cid_t GetReadOnlyTxnCid() { return 0; }

  //****************************************************

  virtual cid_t GetMaxCommittedCid() { return 0; }

};

}
}

