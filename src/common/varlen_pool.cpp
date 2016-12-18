//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_pool.h
//
// Identification: src/backend/common/varlen_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/varlen_pool.h"

namespace peloton {
namespace common {

Buffer::Buffer(size_t buf_size, size_t blk_size) {
  buf_size_ = buf_size;
  buf_begin_ =
      std::shared_ptr<char>(new char[buf_size_], std::default_delete<char[]>());
  blk_size_ = blk_size;
  bitmap_ = std::vector<bool>(MAX_BLOCK_NUM, 0);
  bitmap_.resize(buf_size / blk_size);
  allocated_cnt_ = 0;
}

inline size_t GetAlign(size_t size) {
  if (size == 0) return 1;
  size_t n = size - 1;
  size_t bits = 0;
  while (n > 0) {
    n = n >> 1;
    bits++;
  }
  return bits;
}

VarlenPool::VarlenPool(BackendType backend_type UNUSED_ATTRIBUTE) { Init(); };

VarlenPool::VarlenPool() { Init(); };

// Destroy this pool, and all memory it owns.
VarlenPool::~VarlenPool() {
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    std::list<Buffer>::iterator it;
    for (it = buf_list_[i].begin(); it != buf_list_[i].end(); it++) {
      it = buf_list_[i].erase(it);
    }
  }
}

// Initialize this pool.
void VarlenPool::Init() {
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    buf_list_[i] = std::list<Buffer>();
    empty_cnt_[i] = 0;
  }
  pool_size_ = 0;
}

// Allocate a contiguous block of memory of the given size. If the allocation
// is successful a non-null pointer is returned. If the allocation fails, a
// null pointer will be returned.
// Memory allocated block layout:
//  +------------------+---------+
//  | 8 byte ref count | payload |
//  +------------------+---------+
//                     ^
//                     Returned pointer pointed to the payload
// TODO: Provide good error codes for failure cases.
void *VarlenPool::Allocate(size_t size) {
  void *addr = AllocateHelper(size + sizeof(std::atomic<int64_t>));

  if (addr == nullptr) {
    return nullptr;
  }

  // Init the reference count
  new (addr) std::atomic<int64_t>(1);
  return ((char *) addr) + sizeof(std::atomic<int64_t>);
}

// Internal memory allocation
void *VarlenPool::AllocateHelper(size_t size){
  // Allocate a large block.
  if (size > BUFFER_SIZE) {
    // Lock the corresponding list

    size_t blk_size = 1 << GetAlign(size);
    if (pool_size_ + blk_size > MAX_POOL_SIZE) {
      return nullptr;
    }

    pool_size_ += blk_size;

    Buffer buffer(blk_size, blk_size);
    buffer.allocated_cnt_ = 1;
    buffer.bitmap_[0] = 1;

    list_lock_[LARGE_LIST_ID].Lock();
    buf_list_[LARGE_LIST_ID].push_back(buffer);
    list_lock_[LARGE_LIST_ID].Unlock();
    return buffer.buf_begin_.get();
  }

  size_t list_id = 0;
  if (size <= MIN_BLOCK_SIZE)
    list_id = 0;
  else
    list_id = GetAlign(size) - 4;

  // Lock the corresponding list
  list_lock_[list_id].Lock();

  // Find a buffer that is not full
  size_t block_num = (MAX_BLOCK_NUM >> list_id);
  std::list<Buffer>::iterator it;
  for (it = buf_list_[list_id].begin(); it != buf_list_[list_id].end(); it++) {
    if (it->allocated_cnt_ < block_num) break;
  }

  // If each buffer of the corresponding list is full, add a new buffer
  if (it == buf_list_[list_id].end()) {
    if (pool_size_ + BUFFER_SIZE > MAX_POOL_SIZE) {
      list_lock_[list_id].Unlock();
      return nullptr;
    }
    buf_list_[list_id].emplace_front(BUFFER_SIZE, 1 << (list_id + 4));
    pool_size_ += BUFFER_SIZE;
    buf_list_[list_id].front().allocated_cnt_ = 1;
    buf_list_[list_id].front().bitmap_[0] = 1;
    list_lock_[list_id].Unlock();
    return (buf_list_[list_id].front().buf_begin_.get());
  }

  // Set bitmap and allocate this block
  for (size_t i = 0; i < block_num; i++) {
    if (it->bitmap_[i] == 0) {
      it->bitmap_[i] = 1;
      it->allocated_cnt_++;
      if (it->allocated_cnt_ == 1) empty_cnt_[list_id]--;

      char *res = (reinterpret_cast<char *>(it->buf_begin_.get()) +
                   i * (1 << (list_id + 4)));
      list_lock_[list_id].Unlock();
      return res;
    }
  }

  return nullptr;
}

// Add one to the reference count of a block of memory allocated by the pool
void VarlenPool::AddRefCount(void *ptr){
  GetRefCntPtr(ptr)->fetch_add(1);
}

// Get the reference count of a block of memory allocated by the pool
int64_t VarlenPool::GetRefCount(void *ptr) {
  return GetRefCntPtr(ptr)->load();
}

// Returns the provided chunk of memory back into the pool
void VarlenPool::Free(void *ptr) {
  if (ptr == nullptr) {
    return;
  }

  int64_t ref_cnt = GetRefCntPtr(ptr)->fetch_sub(1);
  PL_ASSERT(ref_cnt > 0);
  if (ref_cnt == 1) {
    FreeHelper(((char *) ptr) - sizeof(std::atomic<int64_t>));
  }
}

// Internal memory deallocation
void VarlenPool::FreeHelper(void *ptr){
  bool freed = 0;
  // Find the buffer where the ptr is allocated
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    list_lock_[i].Lock();
    std::list<Buffer>::iterator it;
    for (it = buf_list_[i].begin(); it != buf_list_[i].end(); it++) {
      int offset = reinterpret_cast<char *>(ptr) -
                   reinterpret_cast<char *>(it->buf_begin_.get());
      // If a large block is allocated to ptr, offset shoud be zero
      if (0 <= offset && size_t(offset) < BUFFER_SIZE) {
        it->bitmap_[offset / it->blk_size_] = 0;
        it->allocated_cnt_--;
        if (it->allocated_cnt_ == 0) {
          empty_cnt_[i]++;
          // If this buffer is a large block or there are enough empty buffers
          if (empty_cnt_[i] > MAX_EMPTY_NUM || i == LARGE_LIST_ID) {
            buf_list_[i].erase(it);
            empty_cnt_[i]--;
          }
        }
        freed = 1;
        break;
      }
      if (freed) break;
    }
    list_lock_[i].Unlock();
  }
}

// Get the total number of bytes that have been allocated by this pool.
uint64_t VarlenPool::GetTotalAllocatedSpace() {
  uint64_t total_size = 0;
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    list_lock_[i].Lock();
    std::list<Buffer>::const_iterator it;
    for (it = buf_list_[i].begin(); it != buf_list_[i].end(); it++) {
      total_size += it->blk_size_ * it->allocated_cnt_;
    }
    list_lock_[i].Unlock();
  }
  return total_size;
}

// Get the maximum size of this pool.
uint64_t VarlenPool::GetMaximumPoolSize() const { return MAX_POOL_SIZE; }

// Get the empty buffer count for a given empty buffer list id
int VarlenPool::GetEmptyCountByListId(size_t list_id) const {
  PL_ASSERT(list_id <= LARGE_LIST_ID);
  if (list_id > LARGE_LIST_ID) {
    return -1;
  }

  return static_cast<int>(empty_cnt_[list_id]);
}

}  // namespace common
}  // namespace peloton
