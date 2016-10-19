//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_pool.h
//
// Identification: src/backend/common/varlen_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "common/platform.h"
#include "common/types.h"

#include <stdint.h>
#include <stdlib.h>
#include <cstddef>
#include <cstring>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

static const size_t BUFFER_SIZE = (1 << 17);  // Bytes
static const size_t MAX_POOL_SIZE = (1L << 60);
static const size_t MIN_BLOCK_SIZE = 16;
static const size_t MAX_BLOCK_NUM = BUFFER_SIZE / MIN_BLOCK_SIZE;
static const size_t MAX_LIST_NUM = 15;
static const size_t LARGE_LIST_ID = MAX_LIST_NUM - 1;

// Release an empty buffer when there are another MAX_EMPTY_NUM empty buffers
static const size_t MAX_EMPTY_NUM = 4;

namespace peloton {
namespace common {

class Buffer {
 public:
  size_t buf_size_;
  size_t blk_size_;
  size_t allocated_cnt_;
  std::shared_ptr<char> buf_begin_;
  std::vector<bool> bitmap_;
  Buffer(size_t buf_size, size_t block_size);
};

// A memory pool that can quickly allocate chunks of memory to clients.
class VarlenPool {
 public:
  // Create and return a new Varlen object of the given size. The caller may
  // optionally provide a pool from which memory can be requested to allocate
  // an object. If no pool is allocated, the implementation is free to acquire
  // memory from anywhere she pleases, including a thread local pool or the
  // global heap memory space.
  VarlenPool(BackendType backend_type);
  VarlenPool();

  // Destroy this pool, and all memory it owns.
  ~VarlenPool();

  // Initialize this pool.
  void Init();

  // Compact two buffers that are less than half full
  void Compact();

  // Allocate a contiguous block of memory of the given size. If the allocation
  // is successful a non-null pointer is returned. If the allocation fails, a
  // null pointer will be returned.
  // TODO: Provide good error codes for failure cases.
  void *Allocate(size_t size);

  // Returns the provided chunk of memory back into the pool
  void Free(void *ptr);

  // Get the total number of bytes that have been allocated by this pool.
  uint64_t GetTotalAllocatedSpace();

  // Get the maximum size of this pool.
  uint64_t GetMaximumPoolSize() const;

 public:
  // All these fields are implementation specific.
  // This class must be thread-safe, very very fast and provide some form of
  // compaction or garbage-collection.

  // Buffer lists
  std::list<Buffer> buf_list_[MAX_LIST_NUM];

  // Total buffer size in the pool
  std::atomic<size_t> pool_size_;

  // Number of empty buffers in each list
  size_t empty_cnt_[MAX_LIST_NUM];

  // Each buffer list has a mutex
  Spinlock list_lock_[MAX_LIST_NUM];
};

}  // namespace common
}  // namespace peloton
