//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// serializer.h
//
// Identification: src/backend/common/serializer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <vector>
#include <string>
#include <exception>
#include <arpa/inet.h>

#include "backend/common/byte_array.h"
#include "backend/common/exception.h"

namespace peloton {

#ifndef __APPLE__  // Ming: OS X has definition of these functions
#define htonll(x)                                         \
  (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | \
   (uint32_t)ntohl(((int32_t)(x >> 32))))
#define ntohll(x)                                         \
  (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | \
   (uint32_t)ntohl(((int32_t)(x >> 32))))
#endif

//===--------------------------------------------------------------------===//
// Abstract class for reading from memory buffers.
//===--------------------------------------------------------------------===//

class SerializeInput {
  SerializeInput(const SerializeInput &) = delete;
  SerializeInput &operator=(const SerializeInput &) = delete;

 protected:
  // Does no initialization. Subclasses must call initialize.
  SerializeInput() : current_(NULL), end_(NULL) {}

  void Initialize(const void *data, size_t length) {
    current_ = reinterpret_cast<const char *>(data);
    end_ = current_ + length;
  }

 public:
  virtual ~SerializeInput(){};

  //===--------------------------------------------------------------------===//
  // 	Deserialization utilities
  //===--------------------------------------------------------------------===//

  inline char ReadChar() { return ReadPrimitive<char>(); }

  inline int8_t ReadByte() { return ReadPrimitive<int8_t>(); }

  inline int16_t ReadShort() {
    int16_t value = ReadPrimitive<int16_t>();
    return ntohs(value);
  }

  inline int32_t ReadInt() {
    int32_t value = ReadPrimitive<int32_t>();
    return ntohl(value);
  }

  inline bool ReadBool() { return ReadByte(); }

  inline char ReadEnumInSingleByte() { return ReadByte(); }

  inline int64_t ReadLong() {
    int64_t value = ReadPrimitive<int64_t>();
    return ntohll(value);
  }

  inline float ReadFloat() {
    int32_t value = ReadPrimitive<int32_t>();
    value = ntohl(value);
    float retval;
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  inline double ReadDouble() {
    int64_t value = ReadPrimitive<int64_t>();
    value = ntohll(value);
    double retval;
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  /// Returns a pointer to the internal data buffer, advancing the read position
  /// by length.
  const void *GetRawPointer(size_t length) {
    const void *result = current_;
    current_ += length;

    assert(current_ <= end_);
    return result;
  }

  /// Copy a string from the buffer.
  inline std::string ReadTextString() {
    int32_t string_length = ReadInt();
    assert(string_length >= 0);
    return std::string(
        reinterpret_cast<const char *>(GetRawPointer(string_length)),
        string_length);
  };

  /// Copy a ByteArray from the buffer.
  inline ByteArray ReadBinaryString() {
    int32_t string_length = ReadInt();
    assert(string_length >= 0);
    return ByteArray(
        reinterpret_cast<const char *>(GetRawPointer(string_length)),
        string_length);
  };

  /// Copy the next length bytes from the buffer to destination.
  inline void ReadBytes(void *destination, size_t length) {
    ::memcpy(destination, GetRawPointer(length), length);
  };

  size_t NumBytesNotYetRead() { return (end_ - current_); }

 private:
  template <typename T>
  T ReadPrimitive() {
    T value;
    ::memcpy(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  /// Current read position.
  const char *current_;

  /// End of the buffer. Valid byte range: current_ <= validPointer < end_.
  const char *end_;
};

//===--------------------------------------------------------------------===//
// Abstract class for writing to memory buffers.
// Subclasses may optionally support resizing.
//===--------------------------------------------------------------------===//

class SerializeOutput {
  SerializeOutput(const SerializeOutput &) = delete;
  SerializeOutput &operator=(const SerializeOutput &) = delete;

 protected:
  SerializeOutput() : buffer_(NULL), position_(0), capacity_(0) {}

  /// Set the buffer to buffer with capacity. Note this does not change the
  /// position.
  void Initialize(void *buffer, size_t capacity) {
    buffer_ = reinterpret_cast<char *>(buffer);
    assert(position_ <= capacity);
    capacity_ = capacity;
  }

  void SetPosition(size_t position) { this->position_ = position; }

 public:
  virtual ~SerializeOutput(){};

  /// Returns a pointer to the beginning of the buffer, for reading the
  /// serialized data.
  const char *Data() const { return buffer_; }

  /// Returns the number of bytes written in to the buffer.
  size_t Size() const { return position_; }

  //===--------------------------------------------------------------------===//
  // 	Serialization utilities
  //===--------------------------------------------------------------------===//

  inline void WriteChar(char value) { WritePrimitive(value); }

  inline void WriteByte(int8_t value) { WritePrimitive(value); }

  inline void WriteShort(int16_t value) {
    WritePrimitive(static_cast<uint16_t>(htons(value)));
  }

  inline void WriteInt(int32_t value) { WritePrimitive(htonl(value)); }

  inline void WriteBool(bool value) {
    WriteByte(value ? int8_t(1) : int8_t(0));
  };

  inline void WriteLong(int64_t value) { WritePrimitive(htonll(value)); }

  inline void WriteFloat(float value) {
    int32_t data;
    memcpy(&data, &value, sizeof(data));
    WritePrimitive(htonl(data));
  }

  inline void WriteDouble(double value) {
    int64_t data;
    memcpy(&data, &value, sizeof(data));
    WritePrimitive(htonll(data));
  }

  inline void WriteEnumInSingleByte(int value) {
    assert(std::numeric_limits<int8_t>::min() <= value &&
           value <= std::numeric_limits<int8_t>::max());
    WriteByte(static_cast<int8_t>(value));
  }

  inline size_t WriteCharAt(size_t position, char value) {
    return WritePrimitiveAt(position, value);
  }

  inline size_t WriteByteAt(size_t position, int8_t value) {
    return WritePrimitiveAt(position, value);
  }

  inline size_t WriteShortAt(size_t position, int16_t value) {
    return WritePrimitiveAt(position, htons(value));
  }

  inline size_t WriteIntAt(size_t position, int32_t value) {
    return WritePrimitiveAt(position, htonl(value));
  }

  inline size_t WriteBoolAt(size_t position, bool value) {
    return WritePrimitiveAt(position, value ? int8_t(1) : int8_t(0));
  }

  inline size_t WriteLongAt(size_t position, int64_t value) {
    return WritePrimitiveAt(position, htonll(value));
  }

  inline size_t WriteFloatAt(size_t position, float value) {
    int32_t data;
    memcpy(&data, &value, sizeof(data));
    return WritePrimitiveAt(position, htonl(data));
  }

  inline size_t WriteDoubleAt(size_t position, double value) {
    int64_t data;
    memcpy(&data, &value, sizeof(data));
    return WritePrimitiveAt(position, htonll(data));
  }

  /// this explicitly accepts char* and length (or ByteArray)
  /// as std::string's implicit construction is unsafe!
  inline void WriteBinaryString(const void *value, size_t length) {
    int32_t string_length = static_cast<int32_t>(length);
    AssureExpand(length + sizeof(string_length));
    int32_t networkOrderLen = htonl(string_length);

    char *current = buffer_ + position_;
    memcpy(current, &networkOrderLen, sizeof(networkOrderLen));
    current += sizeof(string_length);
    memcpy(current, value, length);
    position_ += sizeof(string_length) + length;
  }

  inline void WriteBinaryString(const ByteArray &value) {
    WriteBinaryString(value.data(), value.length());
  }

  inline void WriteTextString(const std::string &value) {
    WriteBinaryString(value.data(), value.size());
  }

  inline void WriteBytes(const void *value, size_t length) {
    AssureExpand(length);
    memcpy(buffer_ + position_, value, length);
    position_ += length;
  }

  inline void WriteZeros(size_t length) {
    AssureExpand(length);
    memset(buffer_ + position_, 0, length);
    position_ += length;
  }

  /// Reserves length bytes of space for writing.
  /// Returns the offset to the bytes.
  size_t ReserveBytes(size_t length) {
    AssureExpand(length);
    size_t offset = position_;
    position_ += length;
    return offset;
  }

  /** Copies length bytes from value to this buffer, starting at
   * offset. Offset should have been obtained from reserveBytes. This
   * does not affect the current write position.
   * @return offset + length */
  inline size_t WriteBytesAt(size_t offset, const void *value, size_t length) {
    assert(offset + length <= position_);
    memcpy(buffer_ + offset, value, length);
    return offset + length;
  }

  static bool IsLittleEndian() {
    static const uint16_t s = 0x0001;
    uint8_t byte;
    memcpy(&byte, &s, 1);
    return byte != 0;
  }

  std::size_t Position() { return position_; }

 protected:
  /** Called when trying to write past the end of the buffer.
   * Subclasses can optionally resize the buffer by calling
   *  initialize. If this function returns and size() < minimum_desired,
   *  the program will crash.  @param minimum_desired the minimum length
   *  the resized buffer needs to have. */
  virtual void Expand(size_t minimum_desired) = 0;

 private:
  template <typename T>
  void WritePrimitive(T value) {
    AssureExpand(sizeof(value));
    memcpy(buffer_ + position_, &value, sizeof(value));
    position_ += sizeof(value);
  }

  template <typename T>
  size_t WritePrimitiveAt(size_t position, T value) {
    return WriteBytesAt(position, &value, sizeof(value));
  }

  inline void AssureExpand(size_t next_write) {
    size_t minimum_desired = position_ + next_write;
    if (minimum_desired > capacity_) {
      Expand(minimum_desired);
    }
    assert(capacity_ >= minimum_desired);
  }

  /// Beginning of the buffer.
  char *buffer_;

 protected:
  /// Current write position in the buffer.
  size_t position_;
  /// Total bytes this buffer can contain.
  size_t capacity_;
};

//===--------------------------------------------------------------------===//
// DERIVED CLASSES
//===--------------------------------------------------------------------===//

/// Implementation of SerializeInput that references an existing buffer.
class ReferenceSerializeInput : public SerializeInput {
 public:
  ReferenceSerializeInput(const void *data, size_t length) {
    Initialize(data, length);
  }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeInput() {}
};

/// Implementation of SerializeInput that makes a copy of the buffer.
class CopySerializeInput : public SerializeInput {
 public:
  CopySerializeInput(const void *data, size_t length)
      : bytes_(reinterpret_cast<const char *>(data), static_cast<int>(length)) {
    Initialize(bytes_.data(), static_cast<int>(length));
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeInput() {}

 private:
  ByteArray bytes_;
};

/// Implementation of SerializeOutput that references an existing buffer. */
class ReferenceSerializeOutput : public SerializeOutput {
 public:
  ReferenceSerializeOutput() : SerializeOutput() {}
  ReferenceSerializeOutput(void *data, size_t length) : SerializeOutput() {
    Initialize(data, length);
  }

  /// Set the buffer to buffer with capacity and sets the position.
  void InitializeWithPosition(void *buffer, size_t capacity, size_t position) {
    SetPosition(position);
    Initialize(buffer, capacity);
  }

  size_t Remaining() { return capacity_ - position_; }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeOutput() {}

 protected:
  /// Reference output can't resize the buffer
  virtual void Expand(__attribute__((unused)) size_t minimum_desired) {
    throw ObjectSizeException(
        "Output from SQL stmt overflowed output/network buffer of 10 MB. "
        "Try a \"limit\" clause or a stronger predicate.");
  }
};

/// Implementation of SerializeOutput that makes a copy of the buffer.
class CopySerializeOutput : public SerializeOutput {
 public:
  // Start with something sizeable so we avoid a ton of initial allocations.
  static const int INITIAL_SIZE = 8 * 1024 * 1024;  // 8 MB

  CopySerializeOutput() : bytes_(INITIAL_SIZE) {
    Initialize(bytes_.data(), INITIAL_SIZE);
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeOutput() {}

  void Reset() { SetPosition(0); }

  int Remaining() { return bytes_.length() - static_cast<int>(Position()); }

 protected:
  /// Resize this buffer to contain twice the amount desired.
  virtual void Expand(size_t minimum_desired) {
    size_t next_capacity = (bytes_.length() + minimum_desired) * 2;
    assert(next_capacity <
           static_cast<size_t>(std::numeric_limits<int>::max()));
    bytes_.copyAndExpand(static_cast<int>(next_capacity));
    Initialize(bytes_.data(), next_capacity);
  }

 private:
  ByteArray bytes_;
};

/**
 * A SerializeOutput class that falls back to allocating a 50 MB buffer if the
 * regular allocation runs out of space. The topend is notified when this
 * occurs.
 */
class FallbackSerializeOutput : public ReferenceSerializeOutput {
 public:
  FallbackSerializeOutput()
      : ReferenceSerializeOutput(), fallbackBuffer_(NULL) {}

  /// Set the buffer to buffer with capacity and sets the position.
  void InitializeWithPosition(void *buffer, size_t capacity, size_t position) {
    if (fallbackBuffer_ != NULL) {
      char *temp = fallbackBuffer_;
      fallbackBuffer_ = NULL;
      delete[] temp;
    }
    SetPosition(position);
    Initialize(buffer, capacity);
  }

  /// Destructor frees the fallback buffer if it is allocated
  virtual ~FallbackSerializeOutput() { delete[] fallbackBuffer_; }

  /// Expand once to a fallback size, and if that doesn't work abort
  virtual void Expand(size_t minimum_desired) {
    /// Leave some space for message headers and such, almost 50 MB
    size_t maxAllocationSize = ((1024 * 1024 * 50) - (1024 * 32));
    if (fallbackBuffer_ != NULL || minimum_desired > maxAllocationSize) {
      if (fallbackBuffer_ != NULL) {
        char *temp = fallbackBuffer_;
        fallbackBuffer_ = NULL;
        delete[] temp;
      }
      throw ObjectSizeException(
          "Output from SQL stmt overflowed output/network buffer of 50 MB. "
          "Try a \"limit\" clause or a stronger predicate.");
    }

    fallbackBuffer_ = new char[maxAllocationSize];
    memcpy(fallbackBuffer_, Data(), position_);
    SetPosition(position_);
    Initialize(fallbackBuffer_, maxAllocationSize);
  }

 private:
  char *fallbackBuffer_;
};

//===--------------------------------------------------------------------===//
// Export serialization interface
//===--------------------------------------------------------------------===//

/*
 * This file defines a crude export serialization interface.
 * The idea is that other code could implement these method
 * names and duck-type their way to a different Export
 * serialization.
 *
 * This doesn't derive from SerializeIO to avoid making the
 * the SerializeIO baseclass functions all virtual.
 */

class ExportSerializeInput {
 public:
  ExportSerializeInput(const void *data, size_t length) {
    current_ = reinterpret_cast<const char *>(data);
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
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  inline double ReadDouble() {
    int64_t value = ReadPrimitive<int64_t>();
    double retval;
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  /// Returns a pointer to the internal data buffer, advancing the Read position
  /// by length.
  const void *getRawPointer(size_t length) {
    const void *result = current_;
    current_ += length;
    assert(current_ <= end_);
    return result;
  }

  /// Copy a string from the buffer.
  inline std::string ReadTextString() {
    int32_t string_length = ReadInt();
    assert(string_length >= 0);
    return std::string(
        reinterpret_cast<const char *>(getRawPointer(string_length)),
        string_length);
  };

  /// Copy the next length bytes from the buffer to destination.
  inline void ReadBytes(void *destination, size_t length) {
    ::memcpy(destination, getRawPointer(length), length);
  };

  /** Move the read position back by bytes. Warning: this method is
        currently unverified and could result in reading before the
        beginning of the buffer. */
  void Unread(size_t bytes) { current_ -= bytes; }

 private:
  template <typename T>
  T ReadPrimitive() {
    T value;
    ::memcpy(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  // Current Read position.
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
    assert(position_ <= capacity);
    capacity_ = capacity;
  }

  virtual ~ExportSerializeOutput(){
      // the serialization wrapper never owns its data buffer
  };

  /// Returns a pointer to the beginning of the buffer, for reading the
  /// serialized data.
  const char *data() const { return buffer_; }

  /// Returns the number of bytes written in to the buffer.
  size_t size() const { return position_; }

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
    int32_t data;
    memcpy(&data, &value, sizeof(data));
    WritePrimitive(data);
  }

  inline void WriteDouble(double value) {
    int64_t data;
    memcpy(&data, &value, sizeof(data));
    WritePrimitive(data);
  }

  inline void WriteEnumInSingleByte(int value) {
    assert(std::numeric_limits<int8_t>::min() <= value &&
           value <= std::numeric_limits<int8_t>::max());
    WriteByte(static_cast<int8_t>(value));
  }

  // This explicitly accepts char* and length (or ByteArray)
  // as std::string's implicit construction is unsafe!
  inline void WriteBinaryString(const void *value, size_t length) {
    int32_t string_length = static_cast<int32_t>(length);
    AssureExpand(length + sizeof(string_length));

    char *current = buffer_ + position_;
    memcpy(current, &string_length, sizeof(string_length));
    current += sizeof(string_length);
    memcpy(current, value, length);
    position_ += sizeof(string_length) + length;
  }

  // inline void WriteTextString(const std::string &value) {
  //	WriteBinaryString(value.data(), value.size());
  //}

  inline void WriteBytes(const void *value, size_t length) {
    AssureExpand(length);
    memcpy(buffer_ + position_, value, length);
    position_ += length;
  }

  inline void WriteZeros(size_t length) {
    AssureExpand(length);
    memset(buffer_ + position_, 0, length);
    position_ += length;
  }

  /** Reserves length bytes of space for writing. Returns the offset to the
   * bytes. */
  size_t reserveBytes(size_t length) {
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
    memcpy(buffer_ + position_, &value, sizeof(value));
    position_ += sizeof(value);
  }

  template <typename T>
  size_t WritePrimitiveAt(size_t position, T value) {
    return WriteBytesAt(position, &value, sizeof(value));
  }

  inline void AssureExpand(size_t next_Write) {
    size_t minimum_desired = position_ + next_Write;
    if (minimum_desired > capacity_) {
      // TODO: die
    }
    assert(capacity_ >= minimum_desired);
  }

  // Beginning of the buffer.
  char *buffer_;

  // No implicit copies
  ExportSerializeOutput(const ExportSerializeOutput &);
  ExportSerializeOutput &operator=(const ExportSerializeOutput &);

 protected:
  // Current Write position in the buffer.
  size_t position_;

  // Total bytes this buffer can contain.
  size_t capacity_;
};

}  // End peloton namespace
