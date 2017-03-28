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
  cid_t DecentralizedEpochManager::EnterEpoch(const size_t thread_id, const bool is_snapshot_read) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    if (is_snapshot_read == true) {

      local_epochs_.at(thread_id)->EnterEpoch(current_global_epoch_snapshot_, is_snapshot_read);

      return (current_global_epoch_snapshot_ << 32) | 0x0;

    } else {

      while (true) {
        uint64_t epoch_id = GetCurrentGlobalEpoch();

        // enter the corresponding local epoch.
        bool rt = local_epochs_.at(thread_id)->EnterEpoch(epoch_id, is_snapshot_read);

        // if successfully entered local epoch
        if (rt == true) {
      
          uint32_t next_txn_id = GetNextTransactionId();

          return (epoch_id << 32) | next_txn_id;
        }
      }

    }
  }

  void DecentralizedEpochManager::ExitEpoch(const size_t thread_id, const cid_t begin_cid) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    uint64_t epoch_id = ExtractEpochId(begin_cid);

    // exit from the corresponding local epoch.
    local_epochs_.at(thread_id)->ExitEpoch(epoch_id);
 
  }


  uint64_t DecentralizedEpochManager::GetMaxCommittedEpochId() {
    uint64_t global_max_committed_eid = UINT64_MAX;
    
    // for all the local epoch contexts, obtain the minimum max committed epoch id.
    for (auto &local_epoch_itr : local_epochs_) {
      
      uint64_t local_max_committed_eid = local_epoch_itr.second->GetMaxCommittedEpochId(current_global_epoch_);
      
      if (local_max_committed_eid < global_max_committed_eid) {
        global_max_committed_eid = local_max_committed_eid;
      }
    }

    // if we observe that global_max_committed_eid is larger than current_global_epoch_ro,
    // then it means the current thread's progress is too slow.
    // we should directly update it to global_max_committed_eid + 1.
    if (global_max_committed_eid != UINT64_MAX && 
        global_max_committed_eid >= current_global_epoch_snapshot_) {
      current_global_epoch_snapshot_ = global_max_committed_eid + 1;
    }

    return global_max_committed_eid;
  }

}
}