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
  cid_t DecentralizedEpochManager::EnterEpochD(const size_t thread_id) {

    PL_ASSERT(local_epoch_contexts_.find(thread_id) != local_epoch_contexts_.end());

    while (true) {
      uint64_t epoch_id = GetCurrentGlobalEpoch();

      // enter the corresponding local epoch.
      bool rt = local_epoch_contexts_.at(thread_id)->EnterLocalEpoch(epoch_id);
      // if successfully enter local epoch
      if (rt == true) {
    
        uint32_t next_txn_id = GetNextTransactionId();

        return (epoch_id << 32) | next_txn_id;
      }
    }
  }

  void DecentralizedEpochManager::ExitEpochD(const size_t thread_id, const size_t begin_cid) {

    PL_ASSERT(local_epoch_contexts_.find(thread_id) != local_epoch_contexts_.end());

    uint64_t epoch_id = ExtractEpochId(begin_cid);

    // enter the corresponding local epoch.
    local_epoch_contexts_.at(thread_id)->ExitLocalEpoch(epoch_id);
 
  }


  uint64_t DecentralizedEpochManager::GetTailEpochId() {
    uint64_t min_epoch_id = std::numeric_limits<uint64_t>::max();
    
    // for all the local epoch contexts, obtain the minimum epoch id.
    for (auto &local_epoch_context : local_epoch_contexts_) {
      
      uint64_t local_epoch_id = local_epoch_context.second->GetTailEpochId(current_global_epoch_);
      
      if (local_epoch_id < min_epoch_id) {
        min_epoch_id = local_epoch_context.second->tail_epoch_id_;
      }
    }

    return min_epoch_id;
  }

}
}