//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_epoch_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <iostream>
#include "index/art_epoch_manager.h"

namespace peloton {
namespace index {
ARTEpochManager::ARTEpochManager(size_t start_GC_threshhold)
    : start_GC_threshhold_(start_GC_threshhold) {
  // Make sure class Data does not exceed one cache line
  static_assert(sizeof(ThreadInfo) <= CACHE_LINE_SIZE,
                "class ThreadInfo size exceeds cache line length!");

  original_pointer_ = new unsigned char[CACHE_LINE_SIZE * (THREAD_NUM + 1)];
  thread_info_list_ = reinterpret_cast<PaddedThreadInfo *>(
      (reinterpret_cast<size_t>(original_pointer_) + CACHE_LINE_SIZE - 1) &
      CACHE_LINE_MASK);

  // Make sure it is aligned
  assert(((size_t)thread_info_list_ % CACHE_LINE_SIZE) == 0);

  // Make sure we do not overflow the chunk of memory
  assert(((size_t)thread_info_list_ + THREAD_NUM * CACHE_LINE_SIZE) <=
         ((size_t)original_pointer_ + (THREAD_NUM + 1) * CACHE_LINE_SIZE));

  // At last call constructor of the class; we use placement new
  for (size_t i = 0; i < THREAD_NUM; i++) {
    new (thread_info_list_ + i) PaddedThreadInfo{*this};
  }
}

ThreadInfo &ARTEpochManager::getThreadInfoByID(int gc_id) {
  return (thread_info_list_ + gc_id)->thread_info_;
}
}
}