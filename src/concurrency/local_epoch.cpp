//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager.cpp
//
// Identification: src/concurrency/local_epoch.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/decentralized_epoch_manager.h"


namespace peloton {
namespace concurrency {

  bool LocalEpoch::EnterEpoch(const uint64_t epoch_id, const bool is_snapshot_read) {

    epoch_lock_.Lock();

    // if this thread is never used or GC'd
    if (epoch_lower_bound_ == UINT64_MAX) {

      epoch_lower_bound_ = epoch_id - 1;
    
    } else if (epoch_lower_bound_ >= epoch_id) {
    
      if (is_snapshot_read == true) {
    
        epoch_lower_bound_ = epoch_id - 1;
    
      } else {
        // epoch_lower_bound_ has already been updated by the GC.
        // have to grab a newer epoch_id.
        epoch_lock_.Unlock();

        return false;
      }
    }

    auto epoch_map_itr = epoch_map_.find(epoch_id);
    // check whether the corresponding epoch exists.
    if (epoch_map_itr == epoch_map_.end()) {

      std::shared_ptr<Epoch> epoch_ptr(new Epoch(epoch_id, 1));

      epoch_queue_.push(epoch_ptr);
      epoch_map_[epoch_id] = epoch_ptr;

    } else {
      epoch_map_itr->second->txn_count_++;
    }

    epoch_lock_.Unlock();

    return true;
  }

  void LocalEpoch::ExitEpoch(const uint64_t epoch_id) {
    epoch_lock_.Lock();

    PL_ASSERT(epoch_map_.find(epoch_id) != epoch_map_.end());
    epoch_map_.at(epoch_id)->txn_count_--;

    while (epoch_queue_.size() != 0) {
      auto &epoch_ptr = epoch_queue_.top();
      if (epoch_ptr->txn_count_ == 0) {
        epoch_map_.erase(epoch_ptr->epoch_id_);
        epoch_queue_.pop();
      } else {
        PL_ASSERT(epoch_ptr->epoch_id_ > epoch_lower_bound_);
        epoch_lower_bound_ = epoch_ptr->epoch_id_ - 1;
        break;
      }
    }

    epoch_lock_.Unlock();
  }

  uint64_t LocalEpoch::GetMaxCommittedEpochId(const uint64_t epoch_id) {
    epoch_lock_.Lock();
    // there's no epoch in this thread.
    // which indicates that this thread is used or GC'd for some time.
    if (epoch_queue_.size() == 0) {
      epoch_lower_bound_ = epoch_id - 1;
    } else {
      while (epoch_queue_.size() != 0) {
        auto &epoch_ptr = epoch_queue_.top();
        if (epoch_ptr->txn_count_ == 0) {
          epoch_map_.erase(epoch_ptr->epoch_id_);
          epoch_queue_.pop();
        } else {
          PL_ASSERT(epoch_ptr->epoch_id_ > epoch_lower_bound_);
          epoch_lower_bound_ = epoch_ptr->epoch_id_ - 1;
          break;
        }
      }
    }
    uint64_t ret = epoch_lower_bound_;
    
    epoch_lock_.Unlock();
    return ret;
  }

}
}
