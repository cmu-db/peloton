//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_thread_context.cpp
//
// Identification: src/codegen/multi_thread_context.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <iostream>

#include "codegen/barrier.h"
#include "codegen/codegen.h"
#include "codegen/multi_thread_context.h"

namespace peloton {
namespace codegen {

void MultiThreadContext::InitInstance(MultiThreadContext *ins, int64_t thread_id, int64_t thread_count, Barrier *bar)
{
  assert(thread_id < thread_count);
  ins->SetThreadId(thread_id);
  ins->SetThreadCount(thread_count);
  ins->SetBarrier(bar);
}

int64_t MultiThreadContext::GetRangeStart(int64_t tile_group_num)
{
  // thread_count_ must be less than tile_group_num
  int64_t slice_size = tile_group_num / std::min(thread_count_, tile_group_num);
  int64_t start = thread_id_ * slice_size;

  return start;
}

int64_t MultiThreadContext::GetRangeEnd(int64_t tile_group_num)
{
  // thread_count_ must be less than tile_group_num
  int64_t slice_size = tile_group_num / std::min(thread_count_, tile_group_num);
  int64_t end;
  if (thread_id_ == thread_count_ - 1) {
    end = tile_group_num;
  } else {
    end = std::min(tile_group_num, (thread_id_ + 1) * slice_size);
  }

  return end;
}

int64_t MultiThreadContext::GetThreadId() {
  return thread_id_;
}

bool MultiThreadContext::BarrierWait() {
  return bar_->BarrierWait();
}

void MultiThreadContext::NotifyMaster() {
    bar_->WorkerFinish();
}

void MultiThreadContext::AddLocalHashTable(utils::OAHashTable *hash_table) {
    bar_->AddLocalHashTable(thread_id_, hash_table);
}

utils::OAHashTable* MultiThreadContext::GetLocalHashTable(int32_t idx) {
    return bar_->GetLocalHashTable(idx);
}

void MultiThreadContext::MergeToGlobalHashTable(utils::OAHashTable *local_ht) {
    static std::atomic<int64_t> turns{0};
    if (thread_id_ == 0) {
        bar_->InitGlobalHashTable(local_ht->KeySize(), local_ht->ValueSize());
    }
    while (thread_id_ != turns);
    LOG_DEBUG("Start Merging local hash table from %ld", thread_id_);
    bar_->MergeToGlobalHashTable(local_ht);
    LOG_DEBUG("Done Merging local hash table from %ld", thread_id_);
    ++turns;
}

utils::OAHashTable* MultiThreadContext::GetGlobalHashTable() {
    return bar_->GetGlobalHashTable();
}


}
}
