//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// local_epoch.h
//
// Identification: src/include/concurrency/local_epoch.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <queue>
#include <vector>
#include <unordered_map>
#include <functional>
#include <cstdint>

#include "common/internal_types.h"
#include "common/synchronization/spin_latch.h"

namespace peloton {
namespace concurrency {

struct Epoch {
  Epoch(const uint64_t epoch_id, const size_t txn_count):
    epoch_id_(epoch_id),
    txn_count_(txn_count) {}

  Epoch(const Epoch& epoch) {
    this->epoch_id_ = epoch.epoch_id_;
    this->txn_count_ = epoch.txn_count_;
  }

  uint64_t epoch_id_;
  size_t txn_count_;
};

struct EpochCompare {
  bool operator()(const std::shared_ptr<Epoch> &lhs, const std::shared_ptr<Epoch> &rhs) {
    return lhs->epoch_id_ > rhs->epoch_id_;
  }
};

class LocalEpoch {

public:
  LocalEpoch(const size_t thread_id) : 
    epoch_id_lower_bound_(UINT64_MAX), 
    thread_id_(thread_id) {}

  bool EnterEpoch(const eid_t epoch_id, const TimestampType ts_type);

  void ExitEpoch(const eid_t epoch_id);
  
  uint64_t GetExpiredEpochId(const uint64_t current_epoch_id);

private:
  common::synchronization::SpinLatch epoch_lock_;
  
  uint64_t epoch_id_lower_bound_;

  size_t thread_id_;
  
  std::priority_queue<std::shared_ptr<Epoch>, std::vector<std::shared_ptr<Epoch>>, EpochCompare> epoch_queue_;
  std::unordered_map<uint64_t, std::shared_ptr<Epoch>> epoch_map_;
};

}
}

