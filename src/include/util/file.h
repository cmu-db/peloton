//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// file.h
//
// Identification: src/include/util/file.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fcntl.h>
#include <memory>
#include <string>

#include "common/exception.h"

namespace peloton {
namespace util {

class File {
 public:
  enum class AccessMode : uint8_t { ReadOnly, WriteOnly, ReadWrite };

  File() : fd_(kInvalid) {}

  ~File() { Close(); }

  // Move
  File(File &&other) noexcept : fd_(kInvalid) { std::swap(fd_, other.fd_); }

  // Move
  File &operator=(File &&other) noexcept {
    // First, close this file
    Close();

    // Swap descriptors
    std::swap(fd_, other.fd_);

    // Done
    return *this;
  }

  void Open(const std::string &name, AccessMode access_mode);

  void Create(const std::string &name);

  void CreateTemp();

  uint64_t Read(void *data, uint64_t len) const;

  uint64_t Write(void *data, uint64_t len) const;

  uint64_t Size() const;

  bool IsOpen() const { return fd_ != kInvalid; }

  void Close();

 private:
  // The file descriptor
  int fd_;

  static constexpr int kInvalid = -1;

 private:
  DISALLOW_COPY(File);
};

}  // namespace util
}  // namespace peloton