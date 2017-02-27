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
#include <functional>

#include "common/macros.h"
#include "type/types.h"
#include "common/logger.h"
#include "common/platform.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include "concurrency/epoch_manager.h"

namespace peloton {
namespace concurrency {

class LocalEpoch {

// struct Epoch {
// public:
//   Epoch(const uint64_t epoch_id, const txn_count):
//     epoch_id_(epoch_id),
//     txn_count_(txn_count) {}

//   uint64_t epoch_id_;
//   size_t txn_count_;
// };

public:
  LocalEpoch(const size_t thread_id) : 
    epoch_buffer_(epoch_buffer_size_),
    head_epoch_id_(0),
    tail_epoch_id_(UINT64_MAX),
    thread_id_(thread_id) {}

  bool EnterEpoch(const uint64_t epoch_id);

  void ExitEpoch(const uint64_t epoch_id);

  uint64_t EnterEpochReadOnly(const uint64_t epoch_id);

  void ExitEpochReadOnly(const uint64_t epoch_id);
  
  uint64_t GetTailEpochId(const uint64_t current_epoch_id);

// private:
  void IncreaseTailEpochId();


  // static bool Compare(Epoch &lhs, Epoch &rhs) {
  //   return lhs.epoch_id_ < rhs.epoch_id_;
  // }

// private:
  // queue size
  // TODO: it is possible that the transaction length is longer than 4986 * EPOCH_LENGTH
  // we should refactor it in the future.
  static const size_t epoch_buffer_size_ = 4096;
  std::vector<size_t> epoch_buffer_;

  // 
  // std::priority_queue<Epoch, std::vector<Epoch>, std::function<bool(Epoch&, Epoch&)>> epoch_queue_(Compare);

  uint64_t head_epoch_id_; // point to the latest epoch that the thread is aware of.
  uint64_t tail_epoch_id_; // point to the oldest epoch that we can reclaim.

  Spinlock head_lock_;
  Spinlock tail_lock_;

  size_t thread_id_;
};

}
}

