//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffered_io.cpp
//
// Identification: src/network/buffered_io.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/buffered_io.h"
#include "common/exception.h"

namespace peloton {
namespace network {
int ReadBuffer::ReadInt(uint8_t len) {
  switch (len) {
    case 1: return ReadRawValue<uint8_t>();
    case 2: return ntohs(ReadRawValue<uint16_t>());
    case 4: return ntohl(ReadRawValue<uint32_t>());
    default: throw NetworkProcessException(
                 "Error when de-serializing: Invalid int size");
  }
}

std::string ReadBuffer::ReadString(size_t len) {
  if (len == 0) throw NetworkProcessException("Unexpected string size: 0");
  auto result = std::string(buf_.begin() + offset_,
                            buf_.begin() + offset_ + (len - 1));
  offset_ += len;
  return result;
}

std::string ReadBuffer::ReadString() {
  // search for the nul terminator
  for (size_t i = offset_; i < size_; i++) {
    if (buf_[i] == 0) {
      auto result = std::string(buf_.begin() + offset_,
                                buf_.begin() + i);
      // +1 because we want to skip nul
      offset_ = i + 1;
      return result;
    }
  }
  // No nul terminator found
  throw NetworkProcessException("Expected nil in read buffer, none found");
}
} // namespace network
} // namespace peloton