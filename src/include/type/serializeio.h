/***************************************************************************
*   Copyright (C) 2008 by H-Store Project                                 *
*   Brown University                                                      *
*   Massachusetts Institute of Technology                                 *
*   Yale University                                                       *
*                                                                         *
* This software may be modified and distributed under the terms           *
* of the MIT license.  See the LICENSE file for details.                  *
*                                                                         *
***************************************************************************/
#ifndef HSTORESERIALIZEIO_H
#define HSTORESERIALIZEIO_H

#include <limits>
#include <string>
#include <vector>

#include "common/macros.h"
#include <vector>
#include <string>
#include "byte_array.h"

namespace peloton {

/** Abstract class for reading from memory buffers. */
class SerializeInput {
 protected:
  /** Does no initialization. Subclasses must call initialize. */
  SerializeInput() : current_(NULL), end_(NULL) {}

  void initialize(const void* data, size_t length) {
    current_ = reinterpret_cast<const char*>(data);
    end_ = current_ + length;
  }

 public:
  /** Pure virtual destructor to permit subclasses to customize destruction. */
  virtual ~SerializeInput() {};

  // functions for deserialization
  inline char ReadChar() { return ReadPrimitive<char>(); }
  inline int8_t ReadByte() { return ReadPrimitive<int8_t>(); }
  inline int16_t ReadShort() { return ReadPrimitive<int16_t>(); }
  inline int32_t ReadInt() { return ReadPrimitive<int32_t>(); }
  inline bool ReadBool() { return ReadByte(); }
  inline char ReadEnumInSingleByte() { return ReadByte(); }
  inline int64_t ReadLong() { return ReadPrimitive<int64_t>(); }
  inline float ReadFloat() { return ReadPrimitive<float>(); }
  inline double ReadDouble() { return ReadPrimitive<double>(); }

  /** Returns a pointer to the internal data buffer, advancing the read position by length. */
  const void* getRawPointer(size_t length) {
    const void* result = current_;
    current_ += length;
    // TODO: Make this a non-optional check?
    PELOTON_ASSERT(current_ <= end_);
    return result;
  }

  /** Copy a string from the buffer. */
  inline std::string ReadTextString() {
    int16_t stringLength = ReadShort();
    PELOTON_ASSERT(stringLength >= 0);
    return std::string(reinterpret_cast<const char*>(getRawPointer(stringLength)),
      stringLength);
  };

  /** Copy a ByteArray from the buffer. */
  inline ByteArray ReadBinaryString() {
    int16_t stringLength = ReadShort();
    PELOTON_ASSERT(stringLength >= 0);
    return ByteArray(reinterpret_cast<const char*>(getRawPointer(stringLength)),
      stringLength);
  };

  /** Copy the next length bytes from the buffer to destination. */
  inline void ReadBytes(void* destination, size_t length) {
    PELOTON_MEMCPY(destination, getRawPointer(length), length); 
  };

  template<typename T> void ReadSimpleTypeVector(std::vector<T>* vec) {
    int size = ReadInt();
    PELOTON_ASSERT(size >= 0);
    vec->resize(size);
    for (int i = 0; i < size; ++i) {
      vec[i] = ReadPrimitive<T>();
    }
  }

  /** Move the read position back by bytes. Warning: this method is currently unverified and
  could result in reading before the beginning of the buffer. */
  // TODO(evanj): Change the implementation to validate this?
  void unread(size_t bytes) {
      current_ -= bytes;
  }

 private:
  template <typename T>
  T ReadPrimitive() {
    T value;
    PELOTON_MEMCPY(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  // Current read position.
  const char* current_;
  // End of the buffer. Valid byte range: current_ <= validPointer < end_.
  const char* end_;

  // No implicit copies
  SerializeInput(const SerializeInput&);
  SerializeInput& operator=(const SerializeInput&);
};

/** Abstract class for writing to memory buffers. Subclasses may optionally support resizing. */
class SerializeOutput {
 protected:
  SerializeOutput() : buffer_(NULL), position_(0), capacity_(0) {}

  /** Set the buffer to buffer with capacity. Note this does not change the position. */
  void initialize(void* buffer, size_t capacity) {
    buffer_ = reinterpret_cast<char*>(buffer);
    PELOTON_ASSERT(position_ <= capacity);
    capacity_ = capacity;
  }
  void setPosition(size_t position) {
    this->position_ = position;
  }

 public:
  virtual ~SerializeOutput() {};

  /** Returns a pointer to the beginning of the buffer, for reading the serialized data. */
  const char *Data() const { return buffer_; }

  /** Returns the number of bytes written in to the buffer. */
  size_t Size() const { return position_; }
  void Reset() { setPosition(0); }
  std::size_t Position() const { return position_; }
  
  inline size_t WriteIntAt(size_t Position, int32_t value) {
    return WritePrimitiveAt<int32_t>(Position, value);
  }
  
  // functions for serialization
  inline void WriteChar(char value) { WritePrimitive(value); }
  inline void WriteByte(int8_t value) { WritePrimitive(value); }
  inline void WriteShort(int16_t value) { WritePrimitive(value); }
  inline void WriteInt(int32_t value) { WritePrimitive(value); }
  inline void WriteBool(bool value) {
    WriteByte(value ? int8_t(1) : int8_t(0));
  };
  inline void WriteLong(int64_t value) { WritePrimitive(value); }
  inline void WriteFloat(float value) { WritePrimitive(value); }
  inline void WriteDouble(double value) { WritePrimitive(value); }   
  inline void WriteEnumInSingleByte(int value) {
    PELOTON_ASSERT(std::numeric_limits<int8_t>::min() <= value &&
      value <= std::numeric_limits<int8_t>::max());
    WriteByte(static_cast<int8_t>(value));
  }

  // this explicitly accepts char* and length (or ByteArray)
  // as std::string's implicit construction is unsafe!
  inline void WriteBinaryString(const void* value, size_t length) {
    PELOTON_ASSERT(length <= std::numeric_limits<int16_t>::max());
    int16_t stringLength = static_cast<int16_t>(length);
    assureExpand(length + sizeof(stringLength));

    char* current = buffer_ + position_;
    PELOTON_MEMCPY(current, &stringLength, sizeof(stringLength));
    current += sizeof(stringLength);
    PELOTON_MEMCPY(current, value, length);
    position_ += sizeof(stringLength) + length;
  }

  inline void WriteBinaryString(const ByteArray &value) {
    WriteBinaryString(value.data(), value.length());
  }

  inline void WriteTextString(const std::string &value) {
    WriteBinaryString(value.data(), value.size());
  }

  inline void WriteBytes(const void *value, size_t length) {
    assureExpand(length);
    PELOTON_MEMCPY(buffer_ + position_, value, length);
    position_ += length;
  }

  inline void WriteZeros(size_t length) {
    assureExpand(length);
    PELOTON_MEMSET(buffer_ + position_, 0, length); 
    position_ += length;
  }

  template<typename T> void WriteSimpleTypeVector(const std::vector<T> &vec) {
    PELOTON_ASSERT(vec.size() <= std::numeric_limits<int>::max());
    int size = static_cast<int>(vec.size());

    // Resize the buffer once
    assureExpand(sizeof(size) + size * sizeof(T));

    WriteInt(size);
    for (int i = 0; i < size; ++i) {
      WritePrimitive(vec[i]);
    }
  }

  /** Reserves length bytes of space for writing. Returns the offset to the bytes. */
  size_t ReserveBytes(size_t length) {
    assureExpand(length);
    size_t offset = position_;
    position_ += length;
    return offset;
  }

  /** Copies length bytes from value to this buffer, starting at offset. Offset should have been
  obtained from reserveBytes. This does not affect the current write position.
  * @return offset + length
  */
  inline size_t WriteBytesAt(size_t offset, const void *value, size_t length) {
    PELOTON_ASSERT(offset + length <= position_);
    PELOTON_MEMCPY(buffer_ + offset, value, length);
    return offset + length;
  }

  template <typename T>
  size_t WritePrimitiveAt(size_t position, T value) {
    return WriteBytesAt(position, &value, sizeof(value));
  }

  static bool isLittleEndian() {
    static const uint16_t s = 0x0001;
    uint8_t byte;
    PELOTON_MEMCPY(&byte, &s, 1);
    return byte != 0;
  }

 protected:
  /** Called when trying to write past the end of the buffer. Subclasses can optionally resize the
  buffer by calling initialize. If this function returns and size() < minimum_desired, the
  program will crash.
  @param minimum_desired the minimum length the resized buffer needs to have.
  */
  virtual void expand(size_t minimum_desired) = 0;

 private:
  template <typename T>
  void WritePrimitive(T value) {
    assureExpand(sizeof(value));
    PELOTON_MEMCPY(buffer_ + position_, &value, sizeof(value)); 
    position_ += sizeof(value);
  }

  inline void assureExpand(size_t next_write) {
    size_t minimum_desired = position_ + next_write;
    if (minimum_desired > capacity_) {
      expand(minimum_desired);
    }
    PELOTON_ASSERT(capacity_ >= minimum_desired);
  }

  // Beginning of the buffer.
  char* buffer_;
  // Current write position in the buffer.
  size_t position_;
  // Total bytes this buffer can contain.
  size_t capacity_;

  // No implicit copies
  SerializeOutput(const SerializeOutput&);
  SerializeOutput& operator=(const SerializeOutput&);
};

/** Implementation of SerializeInput that references an existing buffer. */
class ReferenceSerializeInput : public SerializeInput {
 public:
  ReferenceSerializeInput(const void* data, size_t length) {
    initialize(data, length);
  }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeInput() {}
};

/** Implementation of SerializeInput that makes a copy of the buffer. */
class CopySerializeInput : public SerializeInput {
 public:
  CopySerializeInput(const void* data, size_t length) :
    bytes_(reinterpret_cast<const char*>(data), static_cast<int>(length)) {
    initialize(bytes_.data(), static_cast<int>(length));
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeInput() {}

  //~ SerializeIO() : buffer_(ByteArray(0)), offset_(0) {};
  //~ SerializeIO(ByteArray buffer) : buffer_(buffer), offset_(0) {};
  //~ SerializeIO(ByteArray buffer, int offset) : buffer_(buffer), offset_(offset) {};
  //~ SerializeIO(const char *data, int len) : buffer_(ByteArray(data, len)), offset_(0) {};

 private:
  ByteArray bytes_;
};

/** Implementation of SerializeOutput that references an existing buffer. */
class ReferenceSerializeOutput : public SerializeOutput {
 public:
  ReferenceSerializeOutput() : SerializeOutput() {}
  ReferenceSerializeOutput(void* data, size_t length) : SerializeOutput() {
    initialize(data, length);
  }

  /** Set the buffer to buffer with capacity and sets the position. */
  void initializeWithPosition(void* buffer, size_t capacity, size_t position) {
    setPosition(position);
    initialize(buffer, capacity);
  }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeOutput() {}

 protected:
  /** Reference output can't resize the buffer: just crash. */
  virtual void expand(UNUSED_ATTRIBUTE size_t minimum_desired) {
    //minimum_desired = minimum_desired;
    PELOTON_ASSERT(false);
    abort();
  }
};

/** Implementation of SerializeOutput that makes a copy of the buffer. */
class CopySerializeOutput : public SerializeOutput {
 public:
  CopySerializeOutput() : bytes_(0) {
    initialize(NULL, 0);
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeOutput() {}

 protected:
  /** Resize this buffer to contain twice the amount desired. */
  virtual void expand(size_t minimum_desired) {
    size_t next_capacity = (bytes_.length() + minimum_desired) * 2;
    PELOTON_ASSERT(next_capacity < std::numeric_limits<int>::max());
    bytes_.copyAndExpand(static_cast<int>(next_capacity));
    initialize(bytes_.data(), next_capacity);
  }

 private:
  ByteArray bytes_;
};

}
#endif
