//
// Created by Min Huang on 9/20/17.
//

#include <assert.h>
#include <iostream>
#include "index/Epoche.h"

namespace peloton {
namespace index {
Epoche::Epoche(size_t startGCThreshhold):startGCThreshhold(startGCThreshhold) {
  printf("size of threadinfo: %lu\n", sizeof(ThreadInfo));
  printf("size of padded threadinfo: %lu\n", sizeof(PaddedThreadInfo));

  original_p = new unsigned char[CACHE_LINE_SIZE * (THREAD_NUM + 1)];
  thread_info_list = reinterpret_cast<PaddedThreadInfo *>(
    (reinterpret_cast<size_t>(original_p) + CACHE_LINE_SIZE - 1) & \
        CACHE_LINE_MASK);

  // Make sure it is aligned
  assert(((size_t)thread_info_list % CACHE_LINE_SIZE) == 0);

  // Make sure we do not overflow the chunk of memory
  assert(((size_t)thread_info_list + THREAD_NUM * CACHE_LINE_SIZE) <= \
             ((size_t)original_p + (THREAD_NUM + 1) * CACHE_LINE_SIZE));

  printf("check alignment %lu\n", ((size_t)thread_info_list % CACHE_LINE_SIZE));
  printf("check alignment 2 %lu\n", ((size_t)thread_info_list + THREAD_NUM * CACHE_LINE_SIZE));
  printf("check alignment 3 %lu\n", ((size_t)original_p + (THREAD_NUM + 1) * CACHE_LINE_SIZE));

  // At last call constructor of the class; we use placement new
  for(size_t i = 0; i < THREAD_NUM; i++) {
    new (thread_info_list + i) PaddedThreadInfo{*this};
  }


}

ThreadInfo &Epoche::getThreadInfoByID(std::thread::id thread_id) {
  if (thread_id_to_info_index.find(thread_id) == thread_id_to_info_index.end()) {
    // not found
    size_t info_index = thread_info_index.fetch_add(1);
    thread_info_counter.fetch_add(1);
    thread_id_to_info_index.insert(std::pair<std::thread::id, size_t>(thread_id, info_index));
    return (thread_info_list + info_index)->threadInfo;
  } else {
    return (thread_info_list + thread_id_to_info_index.at(thread_id))->threadInfo;
  }
}
}
}