//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager.h
//
// Identification: src/backend/concurrency/epoch_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <thread>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/platform.h"

namespace peloton {
namespace concurrency {

#define EPOCH_LENGTH 40

  struct Epoch {
    std::atomic<int> txn_ref_count_;
    cid_t max_begin_cid_;

    Epoch() :txn_ref_count_(-1), max_begin_cid_(0){}
  };

  class EpochManager {
  public:
    EpochManager()
      : epoch_queue_(epoch_queue_size_), queue_tail_(0), current_epoch_(0){
      ts_thread_.reset(new std::thread(&EpochManager::Start, this));
      ts_thread_->detach();
    }

    ~EpochManager() {}

//    static uint64_t GetEpoch() {
//
//      COMPILER_MEMORY_FENCE;
//
//      uint64_t ret_ts = curr_epoch_;
//
//      COMPILER_MEMORY_FENCE;
//
//      return ret_ts;
//    }

    size_t EnterEpoch(cid_t begin_cid) {
      auto epoch = current_epoch_;

      // May be dangerous...
      size_t epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].txn_ref_count_++;

      // Set the mac cid in the tuple
      while (true) {
        auto old_max_cid = epoch_queue_[epoch_idx].max_begin_cid_;

        if (begin_cid < old_max_cid) break;

        auto max_cid_ptr = &(epoch_queue_[epoch_idx].max_begin_cid_);
        if (__sync_bool_compare_and_swap(max_cid_ptr, old_max_cid, begin_cid)) break;
      }

      return epoch;
    }

    void ExitEpoch(size_t epoch) {
      auto epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].max_begin_cid_--;

    }
      // assume we store epoch_store max_store previously
    cid_t GetMaxDeadTxnCid() {
      // get tail
      // get current
      // auto stop
      // auto max_local
      // from tail to current
      // update stop and max local until get ref != 0
      // if max_local < max return max
      // return max_local
    }

  private:
    void Start() {
      while (true) {
        // the epoch advances every 40 milliseconds.
        std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));
        ++curr_epoch_;
      }
    }

  private:
    // queue size
    static const size_t epoch_queue_size_ = 2048;

    // Epoch vector
    std::vector<Epoch> epoch_queue_;
    size_t queue_tail_;
    size_t current_epoch_;

    std::unique_ptr<std::thread> ts_thread_;
  };

  
class EpochManagerFactory {
 public:
  static EpochManager &GetInstance() {
    static EpochManager epoch_manager;
    return epoch_manager;
  }
};

}
}