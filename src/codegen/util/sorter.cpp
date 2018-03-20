//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/util/sorter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/sorter.h"

#include <cstring>

#include "common/logger.h"
#include "common/timer.h"
#include "storage/backend_manager.h"

namespace peloton {
namespace codegen {
namespace util {

// Constructor doesn't create the buffer space.  The buffer will be created
// upon initialization.
Sorter::Sorter()
    : buffer_start_(nullptr),
      buffer_pos_(nullptr),
      buffer_end_(nullptr),
      num_tuples_(0),
      tuple_size_(std::numeric_limits<uint32_t>::max()),
      cmp_func_(nullptr) {}

// Destruction calls the destroy method to clean up the resources.
Sorter::~Sorter() { Destroy(); }

// TODO(pmenon): It'd be nice if calls could hint the size of the buffer space
// they'd need when initializing the sorter. Till then ...
void Sorter::Init(ComparisonFunction func, uint32_t tuple_size) {
  tuple_size_ = tuple_size;
  cmp_func_ = func;

  auto &backend_manager = storage::BackendManager::GetInstance();

  buffer_start_ = reinterpret_cast<char *>(
      backend_manager.Allocate(BackendType::MM, kInitialBufferSize));
  buffer_pos_ = buffer_start_;
  buffer_end_ = buffer_start_ + kInitialBufferSize;

  LOG_DEBUG("Initialized Sorter with size %.2lf KB for tuples of size %u bytes",
            kInitialBufferSize / 1024.0, tuple_size_);
}

// Make room for a new tuple of the given size in this sorter, return a buffer
// that has room to store tuple_size bytes. If we don't have enough space, we
// allocate more.
char *Sorter::StoreInputTuple() {
  if (!EnoughSpace(tuple_size_)) {
    Resize();
  }
  char *ret = buffer_pos_;
  buffer_pos_ += tuple_size_;
  num_tuples_++;
  return ret;
}

// Sort the buffer
void Sorter::Sort() {
  // Nothing to sort if nothing has been stored
  if (GetUsedSpace() <= 0) {
    return;
  }

  auto num_tuples = static_cast<unsigned long long>(GetNumTuples());

  // Time it
  Timer<std::milli> timer;
  timer.Start();

  // Sort the sucker
  std::qsort(buffer_start_, num_tuples, tuple_size_,
             reinterpret_cast<int (*)(const void *, const void *)>(cmp_func_));

  timer.Stop();

  LOG_DEBUG("Sorted %llu tuples in %.2f ms", num_tuples, timer.GetDuration());
}

void Sorter::Clear() {
  buffer_pos_ = buffer_start_;
  num_tuples_ = 0;
}

// Release any memory we allocated from the storage manager.
void Sorter::Destroy() {
  if (buffer_start_ != nullptr) {
    LOG_DEBUG("Cleaning up %llu tuples, releasing %.2lf KB",
              (unsigned long long)GetNumTuples(), GetAllocatedSpace() / 1024.0);
    auto &backend_manager = storage::BackendManager::GetInstance();
    backend_manager.Release(BackendType::MM, buffer_start_);
  }
  buffer_start_ = buffer_pos_ = buffer_end_ = nullptr;
}

// Resize the buffer by allocating a space that is double its current size.
// We follow the following steps:
// 1) Calculate the currently allocate size and the currently used space
// 2) Request double the currently allocated space from the storage manager
// 3) Copy the currently used data into the new buffer
// 4) Reset the buffer points into the new buffer space
// 5) Release the old buffer back to the storage manager
void Sorter::Resize() {
  uint64_t curr_alloc_size = GetAllocatedSpace();
  uint64_t curr_used_size = GetUsedSpace();

  // Ensure the current size is a power of two
  PL_ASSERT(curr_alloc_size % 2 == 0);

  // Allocate double the buffer room
  uint64_t next_alloc_size = curr_alloc_size << 1;
  LOG_DEBUG("Resizing sorter from %llu bytes to %llu bytes ...",
            (unsigned long long)curr_alloc_size,
            (unsigned long long)next_alloc_size);

  auto &backend_manager = storage::BackendManager::GetInstance();

  auto *new_buffer_start = reinterpret_cast<char *>(
      backend_manager.Allocate(BackendType::MM, next_alloc_size));

  // Now copy the previous buffer into the new area. Note that we only need
  // to copy over the USED space into the new space.
  PL_MEMCPY(new_buffer_start, buffer_start_, buffer_pos_ - buffer_start_);

  // Set pointers
  char *old_buffer_start = buffer_start_;
  buffer_start_ = new_buffer_start;
  buffer_pos_ = buffer_start_ + curr_used_size;
  buffer_end_ = buffer_start_ + next_alloc_size;

  // Release old buffer
  backend_manager.Release(BackendType::MM, old_buffer_start);
}

//===----------------------------------------------------------------------===//
// Iterators
//===----------------------------------------------------------------------===//

Sorter::Iterator &Sorter::Iterator::operator++() {
  curr_pos_ += tuple_size_;
  return *this;
}

bool Sorter::Iterator::operator==(const Sorter::Iterator &rhs) const {
  return curr_pos_ == rhs.curr_pos_;
}

bool Sorter::Iterator::operator!=(const Sorter::Iterator &rhs) const {
  return !(*this == rhs);
}

const char *Sorter::Iterator::operator*() { return curr_pos_; }

Sorter::Iterator Sorter::begin() {
  return Sorter::Iterator{buffer_start_, tuple_size_};
}

Sorter::Iterator Sorter::end() {
  return Sorter::Iterator{buffer_pos_, tuple_size_};
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
