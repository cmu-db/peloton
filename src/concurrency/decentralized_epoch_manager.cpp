//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager.cpp
//
// Identification: src/concurrency/centralized_epoch_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/decentralized_epoch_manager.h"


namespace peloton {
namespace concurrency {


  // enter epoch with thread id
  cid_t DecentralizedEpochManager::EnterEpoch(const size_t thread_id) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    while (true) {
      uint64_t epoch_id = GetCurrentGlobalEpoch();

      // enter the corresponding local epoch.
      bool rt = local_epochs_.at(thread_id)->EnterEpoch(epoch_id);
      // if successfully enter local epoch
      if (rt == true) {
    
        uint32_t next_txn_id = GetNextTransactionId();

        return (epoch_id << 32) | next_txn_id;
      }
    }
  }

  // enter epoch with thread id
  cid_t DecentralizedEpochManager::EnterEpochRO(const size_t thread_id) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    local_epochs_.at(thread_id)->EnterEpochRO(min_epoch_id_);

    return (min_epoch_id_ << 32) | 0x0;
  }

  void DecentralizedEpochManager::ExitEpoch(const size_t thread_id, const cid_t begin_cid) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    uint64_t epoch_id = ExtractEpochId(begin_cid);

    // exit from the corresponding local epoch.
    local_epochs_.at(thread_id)->ExitEpoch(epoch_id);
 
  }


  uint64_t DecentralizedEpochManager::GetMaxCommittedEpochId() {
    uint64_t min_epoch_id = std::numeric_limits<uint64_t>::max();
    
    // for all the local epoch contexts, obtain the minimum epoch id.
    for (auto &local_epoch : local_epochs_) {
      
      uint64_t local_epoch_id = local_epoch.second->GetMaxCommittedEpochId(current_global_epoch_);
      
      if (local_epoch_id < min_epoch_id) {
        min_epoch_id = local_epoch_id;
      }
    }

    if (min_epoch_id > min_epoch_id_) {
      min_epoch_id_ = min_epoch_id;
    }

    return min_epoch_id;
  }

}
}