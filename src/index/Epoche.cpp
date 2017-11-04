//
// Created by Min Huang on 9/20/17.
//

#include <assert.h>
#include <iostream>
#include "index/Epoche.h"

namespace peloton {
namespace index {
Epoche::Epoche(size_t startGCThreshhold):startGCThreshhold(startGCThreshhold) {
  // Make sure class Data does not exceed one cache line
  static_assert(sizeof(ThreadInfo) <= CACHE_LINE_SIZE,
                "class ThreadInfo size exceeds cache line length!");

  original_p = new unsigned char[CACHE_LINE_SIZE * (THREAD_NUM + 1)];
  thread_info_list = reinterpret_cast<PaddedThreadInfo *>(
    (reinterpret_cast<size_t>(original_p) + CACHE_LINE_SIZE - 1) & \
        CACHE_LINE_MASK);

  // Make sure it is aligned
  assert(((size_t)thread_info_list % CACHE_LINE_SIZE) == 0);

  // Make sure we do not overflow the chunk of memory
  assert(((size_t)thread_info_list + THREAD_NUM * CACHE_LINE_SIZE) <= \
             ((size_t)original_p + (THREAD_NUM + 1) * CACHE_LINE_SIZE));

  // At last call constructor of the class; we use placement new
  for(size_t i = 0; i < THREAD_NUM; i++) {
    new (thread_info_list + i) PaddedThreadInfo{*this};
  }


}

ThreadInfo &Epoche::getThreadInfoByID(int gc_id) {
  return (thread_info_list + gc_id)->threadInfo;
}
}
}