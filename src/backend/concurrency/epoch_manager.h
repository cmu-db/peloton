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

#include "backend/common/types.h"
#include "backend/common/platform.h"

namespace peloton {
namespace concurrency {

#define EPOCH_LENGTH 40

  class EpochManager {
  public:
    EpochManager() {
      ts_thread_.reset(new std::thread(&EpochManager::Start, this));
      ts_thread_->detach();
    }

    ~EpochManager() {}

    static uint64_t GetEpoch() {

      COMPILER_MEMORY_FENCE;
      
      uint64_t ret_ts = curr_epoch_;
      
      COMPILER_MEMORY_FENCE;
      
      return ret_ts;
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
    static volatile cid_t curr_epoch_;
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