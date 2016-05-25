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

#include "backend/common/macros.h"
#include "backend/common/types.h"
#include "backend/common/platform.h"

namespace peloton {
namespace concurrency {

#define EPOCH_LENGTH 40

struct Epoch {
  std::atomic<int> txn_ref_count_;
  cid_t max_cid_;

  Epoch() :txn_ref_count_(0), max_cid_(0){}

  void Init() {
    txn_ref_count_ = 0;
    max_cid_ = 0;
  }
};

class EpochManager {
 public:
  EpochManager()
 : epoch_queue_(epoch_queue_size_), queue_tail_(0), current_epoch_(0), queue_tail_gc(true), max_cid(0), finish_(false) {
    //ts_thread_.reset(new std::thread(&EpochManager::Start, this));
    //ts_thread_->detach();
    ts_thread_ = std::thread(&EpochManager::Start, this);
  }

  void Reset() {
    finish_ = true;
    ts_thread_.join();

    queue_tail_ = 0;
    current_epoch_ = 0;
    epoch_queue_[0].Init();
    queue_tail_gc = true;
    max_cid = 0;

    finish_ = false;
    ts_thread_ = std::thread(&EpochManager::Start, this);
  }

  ~EpochManager() {
    finish_ = true;
    ts_thread_.join();
  }

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
    auto epoch = current_epoch_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].txn_ref_count_++;

    // Set the max cid in the tuple
    auto max_cid_ptr = &(epoch_queue_[epoch_idx].max_cid_);
    AtomicMax(max_cid_ptr, begin_cid);

    return epoch;
  }

  void ExitEpoch(size_t epoch) {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_);

    auto epoch_idx = epoch % epoch_queue_size_;



    epoch_queue_[epoch_idx].txn_ref_count_--;
  }
  // assume we store epoch_store max_store previously
  cid_t GetMaxDeadTxnCid() {
    // TODO:
    // change to:
    // increase tail
    // return max
    auto tail = queue_tail_.load();
    auto head = current_epoch_.load();
    if (head > 0) {
      head--;
    }
    auto res = max_cid;

    auto dead_num = 0;
    while (tail < head) {
      auto idx = tail % epoch_queue_size_;
      // break when meeting running txn
      if (epoch_queue_[idx].txn_ref_count_ > 0) {
        break;
      }

      if(epoch_queue_[idx].max_cid_ > res) {
        res = epoch_queue_[idx].max_cid_;
      }
      tail++;
      dead_num++;
    }
    if (res > max_cid) {
      AtomicMax(&max_cid, res);
    }

    if (dead_num > 32) {
      IncreaseTail();
    }
    return max_cid;
  }

 private:
  void Start() {
    while (!finish_) {
      // the epoch advances every 40 milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));



      auto next_idx = (current_epoch_.load() + 1) % epoch_queue_size_;
      auto tail_idx = queue_tail_.load() % epoch_queue_size_;
      if(next_idx  == tail_idx) {
        // overflow
        // in this case, just increase tail
        IncreaseTail();
        continue;
      }

      // we have to init it first, then increase current epoch
      // otherwise may read dirty data
      epoch_queue_[next_idx].Init();
      current_epoch_++;


      IncreaseTail();
    }
  }


  void IncreaseTail() {
    bool expect = true, desired = false;
    if(!queue_tail_gc.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = current_epoch_.load();
    auto tail = queue_tail_.load();

    while(true) {
      if(tail + 1 == current) {
        break;
      }

      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].txn_ref_count_ > 0) {
        break;
      }

      // save max cid
      auto max = epoch_queue_[idx].max_cid_;
      AtomicMax(&max_cid, max);
      tail++;
    }

    queue_tail_ = tail;

    expect = false;
    desired = true;

    queue_tail_gc.compare_exchange_weak(expect, desired);
    return;
  }


  void AtomicMax(cid_t* addr, cid_t max) {
    while(true) {
      auto old = *addr;
      if(old > max) {
        return;
      }else if ( __sync_bool_compare_and_swap(addr, old, max) ) {
        return;
      }
    }
  }

 private:
  // queue size
  static const size_t epoch_queue_size_ = 2048;


  // Epoch vector
  std::vector<Epoch> epoch_queue_;
  std::atomic<size_t> queue_tail_;
  std::atomic<size_t> current_epoch_;
  std::atomic<bool> queue_tail_gc;
  cid_t max_cid;
  bool finish_;

  std::thread ts_thread_;
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
