//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pool.h
//
// Identification: src/backend/common/pool.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <iostream>
#include <stdint.h>
#include <errno.h>
#include <climits>
#include <string.h>

#include <mutex>

#include "backend/storage/storage_manager.h"

namespace peloton {

static const size_t TEMP_POOL_CHUNK_SIZE = 1024 * 1024;  // 1 MB

//===--------------------------------------------------------------------===//
// Chunk of memory allocated on the heap
//===--------------------------------------------------------------------===//

class Chunk {
 public:
  Chunk() : offset(0), size(0), chunk_data(NULL) {}

  inline Chunk(uint64_t size, void *chunkData)
      : offset(0), size(size), chunk_data(static_cast<char *>(chunkData)) {}

  int64_t getSize() const { return static_cast<int64_t>(size); }

  uint64_t offset;
  uint64_t size;
  char *chunk_data;
};

/// Find next higher power of two
template <class T>
inline T nexthigher(T k) {
  if (k == 0) return 1;
  k--;
  for (uint i = 1; i < sizeof(T) * CHAR_BIT; i <<= 1) k = k | k >> i;
  return k + 1;
}

//===--------------------------------------------------------------------===//
// Memory Pool
//===--------------------------------------------------------------------===//

/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 */
class VarlenPool {
  VarlenPool(const VarlenPool &) = delete;
  VarlenPool &operator=(const VarlenPool &) = delete;

 public:
  VarlenPool()
      : allocation_size(TEMP_POOL_CHUNK_SIZE),
        max_chunk_count(1),
        current_chunk_index(0) {
    Init();
  }

  VarlenPool(uint64_t allocation_size,
       uint64_t max_chunk_count)
      : allocation_size(allocation_size),
        max_chunk_count(static_cast<std::size_t>(max_chunk_count)),
        current_chunk_index(0) {
    Init();
  }

  void Init() {
    char *storage = (char *)storage::StorageManager::GetInstance().Allocate(allocation_size, BACKEND_TYPE_VM);
    chunks.push_back(Chunk(allocation_size, storage));
  }

  ~VarlenPool() {
    for (std::size_t ii = 0; ii < chunks.size(); ii++) {
      storage::StorageManager::GetInstance().Release(chunks[ii].chunk_data, BACKEND_TYPE_VM);
    }
    for (std::size_t ii = 0; ii < oversize_chunks.size(); ii++) {
      storage::StorageManager::GetInstance().Release(oversize_chunks[ii].chunk_data, BACKEND_TYPE_VM);
    }
  }

  /// Allocate a continous block of memory of the specified size.
  void *Allocate(std::size_t size);

  /// Allocate a continous block of memory of the specified size conveniently
  /// initialized to 0s
  void *AllocateZeroes(std::size_t size);

  void Purge();

  int64_t GetAllocatedMemory();

 private:

  const uint64_t allocation_size;
  std::size_t max_chunk_count;
  std::size_t current_chunk_index;
  std::vector<Chunk> chunks;

  /// Oversize chunks that will be freed and not reused.
  std::vector<Chunk> oversize_chunks;

  std::mutex pool_mutex;
};

}  // End peloton namespace
