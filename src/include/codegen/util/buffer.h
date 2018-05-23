//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffer.h
//
// Identification: src/include/codegen/util/buffer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {
namespace codegen {
namespace util {

/**
 * A buffer is a dynamic contiguous region of bytes.
 */
class Buffer {
 private:
  // We (arbitrarily) allocate 4KB of buffer space upon initialization
  static constexpr uint64_t kInitialBufferSize = 4 * 1024;

 public:
  Buffer();

  ~Buffer();

  static void Init(Buffer &buffer);
  
  static void Destroy(Buffer &buffer);

  char *Append(uint32_t num_bytes);

  void Reset();

 private:
  // Make room for new bytes
  void MakeRoomForBytes(uint64_t num_bytes);

  uint64_t AllocatedSpace() const { return buffer_end_ - buffer_start_; }
  uint64_t UsedSpace() const { return buffer_pos_ - buffer_start_; }

 private:
  // The three pointers below track the buffer space where tuples are stored.
  //
  // buffer_start_ - points to the start of the memory space
  // buffer_pos_   - points to where the next tuple insertion is written to
  // buffer_end_   - points to the boundary (i.e., end) of the allocated space
  //
  // The buffer instance can either be initialized or uninitialized.
  //
  // In the uninitialized state, we maintain the invariant:
  //   buffer_start_ == buffer_pos_ == buffer_end_ == NULL
  //
  // In the initialized state, we maintain the invariant:
  //   buffer_start <= buffer_pos < buffer_end.
  //
  char *buffer_start_;
  char *buffer_pos_;
  char *buffer_end_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton