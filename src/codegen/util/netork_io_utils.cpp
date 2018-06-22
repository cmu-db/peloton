//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffer.cpp
//
// Identification: src/codegen/util/buffer.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/buffer.h"

#include <cstring>

#include "common/logger.h"
#include "common/timer.h"
#include "storage/backend_manager.h"

namespace peloton {
namespace codegen {
namespace util {

Buffer::Buffer()
    : buffer_start_(nullptr), buffer_pos_(nullptr), buffer_end_(nullptr) {
  auto &backend_manager = storage::BackendManager::GetInstance();
  buffer_start_ = reinterpret_cast<char *>(
      backend_manager.Allocate(BackendType::MM, kInitialBufferSize));
  buffer_pos_ = buffer_start_;
  buffer_end_ = buffer_start_ + kInitialBufferSize;

  LOG_DEBUG("Initialized buffer with size %.2lf KB",
            kInitialBufferSize / 1024.0);
}

Buffer::~Buffer() {
  if (buffer_start_ != nullptr) {
    LOG_DEBUG("Releasing %.2lf KB of memory", AllocatedSpace() / 1024.0);
    auto &backend_manager = storage::BackendManager::GetInstance();
    backend_manager.Release(BackendType::MM, buffer_start_);
  }
  buffer_start_ = buffer_pos_ = buffer_end_ = nullptr;
}

void Buffer::Init(Buffer &buffer) { new (&buffer) Buffer(); }

void Buffer::Destroy(Buffer &buffer) { buffer.~Buffer(); }

char *Buffer::Append(uint32_t num_bytes) {
  MakeRoomForBytes(num_bytes);
  char *ret = buffer_pos_;
  buffer_pos_ += num_bytes;
  return ret;
}

void Buffer::Reset() { buffer_pos_ = buffer_start_; }

void Buffer::MakeRoomForBytes(uint64_t num_bytes) {
  bool has_room =
      (buffer_start_ != nullptr && buffer_pos_ + num_bytes < buffer_end_);
  if (has_room) {
    return;
  }

  // Need to allocate some space
  uint64_t curr_alloc_size = AllocatedSpace();
  uint64_t curr_used_size = UsedSpace();

  // Ensure the current size is a power of two
  PELOTON_ASSERT(curr_alloc_size % 2 == 0);

  // Allocate double the buffer room
  uint64_t next_alloc_size = curr_alloc_size;
  do {
    next_alloc_size *= 2;
  } while (next_alloc_size < num_bytes);
  LOG_DEBUG("Resizing buffer from %.2lf bytes to %.2lf KB ...",
            curr_alloc_size / 1024.0, next_alloc_size / 1024.0);

  auto &backend_manager = storage::BackendManager::GetInstance();
  auto *new_buffer = reinterpret_cast<char *>(
      backend_manager.Allocate(BackendType::MM, next_alloc_size));

  // Now copy the previous buffer into the new area
  PELOTON_MEMCPY(new_buffer, buffer_start_, curr_used_size);

  // Set pointers
  char *old_buffer_start = buffer_start_;
  buffer_start_ = new_buffer;
  buffer_pos_ = buffer_start_ + curr_used_size;
  buffer_end_ = buffer_start_ + next_alloc_size;

  // Release old buffer
  backend_manager.Release(BackendType::MM, old_buffer_start);
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
