//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// barrier.cpp
//
// Identification: src/codegen/barrier.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "codegen/barrier.h"
#include "codegen/utils/oa_hash_table.h"

namespace peloton {
namespace codegen {

void Barrier::InitInstance(Barrier *ins, uint64_t n_workers)
{
    ins->SetBarrier(new boost::barrier(n_workers));
    ins->SetWorkerCount(n_workers);
    ins->InitGlobalHashTableMergeLock();
}

void Barrier::MasterWait()
{
    while(n_workers_ > 0);
}

void Barrier::MergeToGlobalHashTable(utils::OAHashTable *global_ht, utils::OAHashTable *local_ht) {
  bool expect = false;
  while (!global_hash_table_merge_lock_.compare_exchange_strong(expect, true)) {
    expect = false;
  }

  LOG_DEBUG("Start Merging local hash table, global size: %lu, local size: %lu", global_ht->NumEntries(), local_ht->NumEntries());

  global_ht->Merge(local_ht);

  LOG_DEBUG("Done Merging local hash table, global size: %lu, local size: %lu", global_ht->NumEntries(), local_ht->NumEntries());

  global_hash_table_merge_lock_.store(false);
}

void Barrier::Destroy() {
  if (bar_ != nullptr) {
    delete bar_;
  }
}

}
}
