//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// barrier.h
//
// Identification: src/include/codegen/barrier.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <assert.h>
#include <atomic>
#include <boost/thread/barrier.hpp>

#include "common/logger.h"
#include "codegen/codegen.h"
#include "codegen/utils/oa_hash_table.h"

namespace peloton {
namespace codegen {

class Barrier {
 public:
  static void InitInstance(Barrier *ins, uint64_t n_workers);

  void MasterWait();

  void Destroy();

  inline bool BarrierWait() {
    assert(bar_ != nullptr);
    return bar_->wait();
  }

  inline void WorkerFinish() { --n_workers_; }

  inline void MergeToGlobalHashTable(utils::OAHashTable *global_ht,
                                     utils::OAHashTable *local_ht) {
    while (global_hash_table_merge_lock_.exchange(true)) {
    }
    global_ht->Merge(local_ht);
    global_hash_table_merge_lock_ = false;
  }

 private:
  Barrier()
      : bar_(nullptr), n_workers_(0), global_hash_table_merge_lock_(false) {}

  inline void SetBarrier(boost::barrier *bar) { bar_ = bar; }

  inline void SetWorkerCount(uint64_t n_workers) { n_workers_ = n_workers; }

  inline void InitGlobalHashTableMergeLock() { global_hash_table_merge_lock_ = false; }

  boost::barrier *bar_;
  std::atomic<uint64_t> n_workers_;
  std::atomic<bool> global_hash_table_merge_lock_;
};
}
}
