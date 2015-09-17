//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pool.cpp
//
// Identification: src/backend/common/pool.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/pool.h"

namespace peloton {

/// Allocate a continous block of memory of the specified size.
void *VarlenPool::Allocate(std::size_t size) {
  void *retval = nullptr;

  // Protect using pool lock
  // TODO: Can be make locking more fine-grained
  {
    std::lock_guard<std::mutex> pool_lock(pool_mutex);

    /// See if there is space in the current chunk
    Chunk *current_chunk = &chunks[current_chunk_index];
    if (size > current_chunk->size - current_chunk->offset) {
      /// Not enough space. Check if it is greater than our allocation size.
      if (size > allocation_size) {
        /// Allocate an oversize chunk that will not be reused.
        char *storage = (char *)backend->Allocate(size);
        oversize_chunks.push_back(Chunk(nexthigher(size), storage));
        Chunk &newChunk = oversize_chunks.back();
        newChunk.offset = size;
        return newChunk.chunk_data;
      }

      /// Check if there is an already allocated chunk we can use.
      current_chunk_index++;

      if (current_chunk_index < chunks.size()) {
        current_chunk = &chunks[current_chunk_index];
        current_chunk->offset = size;
        return current_chunk->chunk_data;
      } else {
        /// Need to allocate a new chunk
        char *storage = (char *)backend->Allocate(allocation_size);
        chunks.push_back(Chunk(allocation_size, storage));
        Chunk &new_chunk = chunks.back();
        new_chunk.offset = size;
        return new_chunk.chunk_data;
      }
    }

    /// Get the offset into the current chunk. Then increment the
    /// offset counter by the amount being allocated.
    retval = current_chunk->chunk_data + current_chunk->offset;
    current_chunk->offset += size;

    /// Ensure 8 byte alignment of future allocations
    current_chunk->offset += (8 - (current_chunk->offset % 8));
    if (current_chunk->offset > current_chunk->size) {
      current_chunk->offset = current_chunk->size;
    }
  }

  return retval;
}

/// Allocate a continous block of memory of the specified size conveniently
/// initialized to 0s
void *VarlenPool::AllocateZeroes(std::size_t size) {
  return ::memset(Allocate(size), 0, size);
}

void VarlenPool::Purge() {
  // Protect using pool lock
  {
    std::lock_guard<std::mutex> pool_lock(pool_mutex);

    /// Erase any oversize chunks that were allocated
    const std::size_t numOversizeChunks = oversize_chunks.size();
    for (std::size_t ii = 0; ii < numOversizeChunks; ii++) {
      backend->Free(oversize_chunks[ii].chunk_data);
    }
    oversize_chunks.clear();

    /// Set the current chunk to the first in the list
    current_chunk_index = 0;
    std::size_t num_chunks = chunks.size();

    /// If more then maxChunkCount chunks are allocated erase all extra chunks
    if (num_chunks > max_chunk_count) {
      for (std::size_t ii = max_chunk_count; ii < num_chunks; ii++) {
        backend->Free(chunks[ii].chunk_data);
      }
      chunks.resize(max_chunk_count);
    }

    num_chunks = chunks.size();
    for (std::size_t ii = 0; ii < num_chunks; ii++) {
      chunks[ii].offset = 0;
    }
  }
}

int64_t VarlenPool::GetAllocatedMemory() {
  int64_t total = 0;
  total += chunks.size() * allocation_size;
  for (uint32_t i = 0; i < oversize_chunks.size(); i++) {
    total += oversize_chunks[i].getSize();
  }
  return total;
}

}  // End peloton namespace
