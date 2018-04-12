//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// serializer.h
//
// Identification: src/include/type/serializer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <arpa/inet.h>
#include <arpa/inet.h>
#include <exception>
#include <iomanip>
#include <limits>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/macros.h"

#include "type/byte_array.h"

namespace peloton {

/*
 * This file defines a crude Export serialization interface.
 * The idea is that other code could implement these method
 * names and duck-type their way to a different Export
 * serialization .. maybe doing some dynamic symbol finding
 * for a pluggable Export serializer. It's a work in progress.
 *
 * This doesn't derive from serializeio to avoid making the
 * the serialize IO baseclass functions all virtual.
 */

class ExportSerializeInput {
 public:
  ExportSerializeInput(const void *Data, size_t length) {
    current_ = reinterpret_cast<const char *>(Data);
    end_ = current_ + length;
  }

  virtual ~ExportSerializeInput(){};

  inline char ReadChar() { return ReadPrimitive<char>(); }

  inline int8_t ReadByte() { return ReadPrimitive<int8_t>(); }

  inline int16_t ReadShort() { return ReadPrimitive<int16_t>(); }

  inline int32_t ReadInt() { return ReadPrimitive<int32_t>(); }

  inline bool ReadBool() { return ReadByte(); }

  inline char ReadEnumInSingleByte() { return ReadByte(); }

  inline int64_t ReadLong() { return ReadPrimitive<int64_t>(); }

  inline float ReadFloat() {
    int32_t value = ReadPrimitive<int32_t>();
    float retval;
    PELOTON_MEMCPY(&retval, &value, sizeof(retval));
    return retval;
  }

  inline double ReadDouble() {
    int64_t value = ReadPrimitive<int64_t>();
    double retval;
    PELOTON_MEMCPY(&retval, &value, sizeof(retval));
    return retval;
  }

  /** Returns a pointer to the internal Data buffer, advancing the Read Position
   * by length. */
  const void *getRawPointer(size_t length) {
    const void *result = current_;
    current_ += length;
    PELOTON_ASSERT(current_ <= end_);
    return result;
  }

  /** Copy a string from the buffer. */
  inline std::string ReadTextString() {
    int32_t stringLength = ReadInt();
    PELOTON_ASSERT(stringLength >= 0);
    return std::string(
        reinterpret_cast<const char *>(getRawPointer(stringLength)),
        stringLength);
  };

  /** Copy the next length bytes from the buffer to destination. */
  inline void ReadBytes(void *destination, size_t length) {
    ::PELOTON_MEMCPY(destination, getRawPointer(length), length);
  };

  /** Move the Read Position back by bytes. Warning: this method is
  currently unverified and could result in Reading before the
  beginning of the buffer. */
  // TODO(evanj): Change the implementation to validate this?
  void Unread(size_t bytes) { current_ -= bytes; }

 private:
  template <typename T>
  T ReadPrimitive() {
    T value;
    ::PELOTON_MEMCPY(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  // Current Read Position.
  const char *current_;

  // End of the buffer. Valid byte range: current_ <= validPointer < end_.
  const char *end_;

  // No implicit copies
  ExportSerializeInput(const ExportSerializeInput &);
  ExportSerializeInput &operator=(const ExportSerializeInput &);
};

class ExportSerializeOutput {
 public:
  ExportSerializeOutput(void *buffer, size_t capacity)
      : buffer_(NULL), position_(0), capacity_(0) {
    buffer_ = reinterpret_cast<char *>(buffer);
    PELOTON_ASSERT(position_ <= capacity);
    capacity_ = capacity;
  }

  virtual ~ExportSerializeOutput(){
      // the serialization wrapper never owns its Data buffer
  };

  /** Returns a pointer to the beginning of the buffer, for Reading the
   * serialized Data. */
  const char *Data() const { return buffer_; }

  /** Returns the number of bytes written in to the buffer. */
  size_t Size() const { return position_; }

  // functions for serialization
  inline void WriteChar(char value) { WritePrimitive(value); }

  inline void WriteByte(int8_t value) { WritePrimitive(value); }

  inline void WriteShort(int16_t value) {
    WritePrimitive(static_cast<uint16_t>(value));
  }

  inline void WriteInt(int32_t value) { WritePrimitive(value); }

  inline void WriteBool(bool value) {
    WriteByte(value ? int8_t(1) : int8_t(0));
  };

  inline void WriteLong(int64_t value) { WritePrimitive(value); }

  inline void WriteFloat(float value) {
    int32_t Data;
    PELOTON_MEMCPY(&Data, &value, sizeof(Data));
    WritePrimitive(Data);
  }

  inline void WriteDouble(double value) {
    int64_t Data;
    PELOTON_MEMCPY(&Data, &value, sizeof(Data));
    WritePrimitive(Data);
  }

  inline void WriteEnumInSingleByte(int value) {
    PELOTON_ASSERT(std::numeric_limits<int8_t>::min() <= value &&
              value <= std::numeric_limits<int8_t>::max());
    WriteByte(static_cast<int8_t>(value));
  }

  // this explicitly accepts char* and length (or ByteArray)
  // as std::string's implicit construction is unsafe!
  inline void WriteBinaryString(const void *value, size_t length) {
    int32_t stringLength = static_cast<int32_t>(length);
    AssureExpand(length + sizeof(stringLength));

    char *current = buffer_ + position_;
    PELOTON_MEMCPY(current, &stringLength, sizeof(stringLength));
    current += sizeof(stringLength);
    PELOTON_MEMCPY(current, value, length);
    position_ += sizeof(stringLength) + length;
  }

  inline void WriteTextString(const std::string &value) {
    WriteBinaryString(value.data(), value.size());
  }

  inline void WriteBytes(const void *value, size_t length) {
    AssureExpand(length);
    PELOTON_MEMCPY(buffer_ + position_, value, length);
    position_ += length;
  }

  inline void WriteZeros(size_t length) {
    AssureExpand(length);
    PELOTON_MEMSET(buffer_ + position_, 0, length);
    position_ += length;
  }

  /** Reserves length bytes of space for writing. Returns the offset to the
   * bytes. */
  size_t ReserveBytes(size_t length) {
    AssureExpand(length);
    size_t offset = position_;
    position_ += length;
    return offset;
  }

  std::size_t Position() { return position_; }

  void Position(std::size_t pos) { position_ = pos; }

 private:
  template <typename T>
  void WritePrimitive(T value) {
    AssureExpand(sizeof(value));
    PELOTON_MEMCPY(buffer_ + position_, &value, sizeof(value));
    position_ += sizeof(value);
  }

  template <typename T>
  size_t WritePrimitiveAt(size_t Position, T value) {
    return WriteBytesAt(Position, &value, sizeof(value));
  }

  inline void AssureExpand(size_t next_Write) {
    size_t minimum_desired = position_ + next_Write;
    if (minimum_desired > capacity_) {
      // TODO: die
    }
    PELOTON_ASSERT(capacity_ >= minimum_desired);
  }

  // Beginning of the buffer.
  char *buffer_;

  // No implicit copies
  ExportSerializeOutput(const ExportSerializeOutput &);
  ExportSerializeOutput &operator=(const ExportSerializeOutput &);

 protected:
  // Current Write Position in the buffer.
  size_t position_;
  // Total bytes this buffer can contain.
  size_t capacity_;
};

}  // namespace peloton
