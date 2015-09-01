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
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <exception>
#include <arpa/inet.h>
#include <cassert>
#include <arpa/inet.h>

#include "backend/common/byte_array.h"
#include "backend/common/exception.h"

#include <boost/ptr_container/ptr_vector.hpp>

namespace peloton {

#ifdef __DARWIN_OSSwapInt64 // for darwin/macosx

#define htonll(x) __DARWIN_OSSwapInt64(x)
#define ntohll(x) __DARWIN_OSSwapInt64(x)

#else // unix in general

#undef htons
#undef ntohs
#define htons(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))
#define ntohs(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))

#ifdef __bswap_64 // recent linux

#define htonll(x) static_cast<uint64_t>(__bswap_constant_64(x))
#define ntohll(x) static_cast<uint64_t>(__bswap_constant_64(x))

#else // unix in general again

#define htonll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))
#define ntohll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))

#endif // __bswap_64

#endif // unix or mac

//===--------------------------------------------------------------------===//
// Abstract class for Reading from memory buffers.
//===--------------------------------------------------------------------===//

/** Abstract class for Reading from memory buffers. */
template <Endianess E> class SerializeInput {
 protected:
  /** Does no initialization. Subclasses must call Initialize. */
  SerializeInput() : current_(NULL), end_(NULL) {}

  void Initialize(const void* Data, size_t length) {
    current_ = reinterpret_cast<const char*>(Data);
    end_ = current_ + length;
  }

 public:
  virtual ~SerializeInput() {};

  // functions for deserialization
  inline char ReadChar() {
    return ReadPrimitive<char>();
  }

  inline int8_t ReadByte() {
    return ReadPrimitive<int8_t>();
  }

  inline int16_t ReadShort() {
    int16_t value = ReadPrimitive<int16_t>();
    if (E == BYTE_ORDER_BIG_ENDIAN) {
      return ntohs(value);
    } else {
      return value;
    }
  }

  inline int32_t ReadInt() {
    int32_t value = ReadPrimitive<int32_t>();
    if (E == BYTE_ORDER_BIG_ENDIAN) {
      return ntohl(value);
    } else {
      return value;
    }
  }

  inline bool ReadBool() {
    return ReadByte();
  }

  inline char ReadEnumInSingleByte() {
    return ReadByte();
  }

  inline int64_t ReadLong() {
    int64_t value = ReadPrimitive<int64_t>();
    if (E == BYTE_ORDER_BIG_ENDIAN) {
      return ntohll(value);
    } else {
      return value;
    }
  }

  inline float ReadFloat() {
    int32_t value = ReadPrimitive<int32_t>();
    if (E == BYTE_ORDER_BIG_ENDIAN) {
      value = ntohl(value);
    }
    float retval;
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  inline double ReadDouble() {
    int64_t value = ReadPrimitive<int64_t>();
    if (E == BYTE_ORDER_BIG_ENDIAN) {
      value = ntohll(value);
    }
    double retval;
    memcpy(&retval, &value, sizeof(retval));
    return retval;
  }

  /** Returns a pointer to the internal Data buffer, advancing the Read Position by length. */
  const char* GetRawPointer(size_t length) {
    const char* result = current_;
    current_ += length;
    // TODO: Make this a non-optional check?
    assert(current_ <= end_);
    return result;
  }

  const char* GetRawPointer() {
    return current_;
  }

  /** Copy a string from the buffer. */
  inline std::string ReadTextString() {
    int32_t stringLength = ReadInt();
    assert(stringLength >= 0);
    return std::string(reinterpret_cast<const char*>(GetRawPointer(stringLength)),
                       stringLength);
  };

  /** Copy a ByteArray from the buffer. */
  inline ByteArray ReadBinaryString() {
    int32_t stringLength = ReadInt();
    assert(stringLength >= 0);
    return ByteArray(reinterpret_cast<const char*>(GetRawPointer(stringLength)),
                     stringLength);
  };

  /** Copy the next length bytes from the buffer to destination. */
  inline void ReadBytes(void* destination, size_t length) {
    ::memcpy(destination, GetRawPointer(length), length);
  };

  /** Write the buffer as hex bytes for debugging */
  std::string fullBufferStringRep();

  /** Move the Read Position back by bytes. Warning: this method is
    currently unverified and could result in Reading before the
    beginning of the buffer. */
  // TODO(evanj): Change the implementation to validate this?
  void UnRead(size_t bytes) {
    current_ -= bytes;
  }

  bool HasRemaining() {
    return current_ < end_;
  }

 private:
  template <typename T>
  T ReadPrimitive() {
    T value;
    ::memcpy(&value, current_, sizeof(value));
    current_ += sizeof(value);
    return value;
  }

  // Current Read Position.
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

  /** Set the buffer to buffer with capacity. Note this does not change the Position. */
  void Initialize(void* buffer, size_t capacity) {
    buffer_ = reinterpret_cast<char*>(buffer);
    assert(position_ <= capacity);
    capacity_ = capacity;
  }
  void SetPosition(size_t Position) {
    this->position_ = Position;
  }
 public:
  virtual ~SerializeOutput() {};

  /** Returns a pointer to the beginning of the buffer, for Reading the serialized Data. */
  const char* Data() const { return buffer_; }

  /** Returns the number of bytes written in to the buffer. */
  size_t Size() const { return position_; }

  // functions for serialization
  inline void WriteChar(char value) {
    WritePrimitive(value);
  }

  inline void WriteByte(int8_t value) {
    WritePrimitive(value);
  }

  inline void WriteShort(int16_t value) {
    WritePrimitive(static_cast<uint16_t>(htons(value)));
  }

  inline void WriteInt(int32_t value) {
    WritePrimitive(htonl(value));
  }

  inline void WriteBool(bool value) {
    WriteByte(value ? int8_t(1) : int8_t(0));
  };

  inline void WriteLong(int64_t value) {
    WritePrimitive(htonll(value));
  }

  inline void WriteFloat(float value) {
    int32_t Data;
    memcpy(&Data, &value, sizeof(Data));
    WritePrimitive(htonl(Data));
  }

  inline void WriteDouble(double value) {
    int64_t Data;
    memcpy(&Data, &value, sizeof(Data));
    WritePrimitive(htonll(Data));
  }

  inline void WriteEnumInSingleByte(int value) {
    assert(std::numeric_limits<int8_t>::min() <= value &&
           value <= std::numeric_limits<int8_t>::max());
    WriteByte(static_cast<int8_t>(value));
  }

  inline size_t WriteCharAt(size_t Position, char value) {
    return WritePrimitiveAt(Position, value);
  }

  inline size_t WriteByteAt(size_t Position, int8_t value) {
    return WritePrimitiveAt(Position, value);
  }

  inline size_t WriteShortAt(size_t Position, int16_t value) {
    return WritePrimitiveAt(Position, htons(value));
  }

  inline size_t WriteIntAt(size_t Position, int32_t value) {
    return WritePrimitiveAt(Position, htonl(value));
  }

  inline size_t WriteBoolAt(size_t Position, bool value) {
    return WritePrimitiveAt(Position, value ? int8_t(1) : int8_t(0));
  }

  inline size_t WriteLongAt(size_t Position, int64_t value) {
    return WritePrimitiveAt(Position, htonll(value));
  }

  inline size_t WriteFloatAt(size_t Position, float value) {
    int32_t Data;
    memcpy(&Data, &value, sizeof(Data));
    return WritePrimitiveAt(Position, htonl(Data));
  }

  inline size_t WriteDoubleAt(size_t Position, double value) {
    int64_t Data;
    memcpy(&Data, &value, sizeof(Data));
    return WritePrimitiveAt(Position, htonll(Data));
  }

  // this explicitly accepts char* and length (or ByteArray)
  // as std::string's implicit construction is unsafe!
  inline void WriteBinaryString(const void* value, size_t length) {
    int32_t stringLength = static_cast<int32_t>(length);
    AssureExpand(length + sizeof(stringLength));

    // do a newtork order conversion
    int32_t networkOrderLen = htonl(stringLength);

    char* current = buffer_ + position_;
    memcpy(current, &networkOrderLen, sizeof(networkOrderLen));
    current += sizeof(stringLength);
    memcpy(current, value, length);
    position_ += sizeof(stringLength) + length;
  }

  inline void WriteBinaryString(const ByteArray &value) {
    WriteBinaryString(value.Data(), value.Length());
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

  /** Reserves length bytes of space for writing. Returns the offset to the bytes. */
  size_t ReserveBytes(size_t length) {
    AssureExpand(length);
    size_t offset = position_;
    position_ += length;
    return offset;
  }

  /** Copies length bytes from value to this buffer, starting at
    offset. Offset should have been obtained from ReserveBytes. This
    does not affect the current Write Position.  * @return offset +
    length */
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

  std::size_t Position() const {
    return position_;
  }

 protected:

  /** Called when trying to Write past the end of the
    buffer. Subclasses can optionally resize the buffer by calling
    Initialize. If this function returns and Size() < minimum_desired,
    the program will crash.  @param minimum_desired the minimum length
    the resized buffer needs to have. */
  virtual void Expand(size_t minimum_desired) = 0;

 private:
  template <typename T>
  void WritePrimitive(T value) {
    AssureExpand(sizeof(value));
    memcpy(buffer_ + position_, &value, sizeof(value));
    position_ += sizeof(value);
  }

  template <typename T>
  size_t WritePrimitiveAt(size_t Position, T value) {
    return WriteBytesAt(Position, &value, sizeof(value));
  }

  inline void AssureExpand(size_t next_Write) {
    size_t minimum_desired = position_ + next_Write;
    if (minimum_desired > capacity_) {
      Expand(minimum_desired);
    }
    assert(capacity_ >= minimum_desired);
  }

  // Beginning of the buffer.
  char* buffer_;

  // No implicit copies
  SerializeOutput(const SerializeOutput&);
  SerializeOutput& operator=(const SerializeOutput&);

 protected:
  // Current Write Position in the buffer.
  size_t position_;
  // Total bytes this buffer can contain.
  size_t capacity_;
};

/** Implementation of SerializeInput that references an existing buffer. */
template <Endianess E> class ReferenceSerializeInput : public SerializeInput<E> {
 public:
  ReferenceSerializeInput(const void* Data, size_t length) {
    this->Initialize(Data, length);
  }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeInput() {}
};

/** Implementation of SerializeInput that makes a copy of the buffer. */
template <Endianess E> class CopySerializeInput : public SerializeInput<E> {
 public:
  CopySerializeInput(const void* Data, size_t length) :
    bytes_(reinterpret_cast<const char*>(Data), static_cast<int>(length)) {
    this->Initialize(bytes_.Data(), static_cast<int>(length));
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeInput() {}

 private:
  ByteArray bytes_;
};

#ifndef SERIALIZE_IO_DECLARATIONS
#define SERIALIZE_IO_DECLARATIONS
typedef SerializeInput<BYTE_ORDER_BIG_ENDIAN> SerializeInputBE;
typedef SerializeInput<BYTE_ORDER_LITTLE_ENDIAN> SerializeInputLE;

typedef ReferenceSerializeInput<BYTE_ORDER_BIG_ENDIAN> ReferenceSerializeInputBE;
typedef ReferenceSerializeInput<BYTE_ORDER_LITTLE_ENDIAN> ReferenceSerializeInputLE;

typedef CopySerializeInput<BYTE_ORDER_BIG_ENDIAN> CopySerializeInputBE;
typedef CopySerializeInput<BYTE_ORDER_LITTLE_ENDIAN> CopySerializeInputLE;
#endif

/** Implementation of SerializeOutput that references an existing buffer. */
class ReferenceSerializeOutput : public SerializeOutput {
 public:
  ReferenceSerializeOutput() : SerializeOutput() {
  }
  ReferenceSerializeOutput(void* Data, size_t length) : SerializeOutput() {
    Initialize(Data, length);
  }

  /** Set the buffer to buffer with capacity and sets the Position. */
  virtual void InitializeWithPosition(void* buffer, size_t capacity, size_t Position) {
    SetPosition(Position);
    Initialize(buffer, capacity);
  }

  size_t Remaining() const {
    return capacity_ - position_;
  }

  // Destructor does nothing: nothing to clean up!
  virtual ~ReferenceSerializeOutput() {}

 protected:
  /** Reference output can't resize the buffer: Frowny-Face. */
  virtual void Expand(__attribute__((unused)) size_t minimum_desired) {
    throw Exception("Output from SQL stmt overflowed output/network buffer of 10mb. "
        "Try a \"limit\" clause or a stronger predicate.");
  }
};

/*
 * A serialize output class that falls back to allocating a 50 meg buffer
 * if the regular allocation runs out of space. The topend is notified when this occurs.
 */
class FallbackSerializeOutput : public ReferenceSerializeOutput {
 public:
  FallbackSerializeOutput() :
    ReferenceSerializeOutput(), fallbackBuffer_(NULL) {
  }

  /** Set the buffer to buffer with capacity and sets the Position. */
  void InitializeWithPosition(void* buffer, size_t capacity, size_t Position) {
    if (fallbackBuffer_ != NULL) {
      char *temp = fallbackBuffer_;
      fallbackBuffer_ = NULL;
      delete []temp;
    }
    SetPosition(Position);
    Initialize(buffer, capacity);
  }

  // Destructor frees the fallback buffer if it is allocated
  virtual ~FallbackSerializeOutput() {
    delete []fallbackBuffer_;
  }

  /** Expand once to a fallback Size, and if that doesn't work abort */
  void Expand(size_t minimum_desired);
 private:
  char *fallbackBuffer_;
};

/** Implementation of SerializeOutput that makes a copy of the buffer. */
class CopySerializeOutput : public SerializeOutput {
 public:
  // Start with something sizeable so we avoid a ton of initial
  // allocations.
  static const int INITIAL_SIZE = 8388608;

  CopySerializeOutput() : bytes_(INITIAL_SIZE) {
    Initialize(bytes_.Data(), INITIAL_SIZE);
  }

  // Destructor frees the ByteArray.
  virtual ~CopySerializeOutput() {}

  void Reset() {
    SetPosition(0);
  }

  size_t Remaining() const {
    return bytes_.Length() - static_cast<int>(Position());
  }

 protected:
  /** Resize this buffer to contain twice the amount desired. */
  virtual void Expand(size_t minimum_desired) {
    size_t next_capacity = (bytes_.Length() + minimum_desired) * 2;
    assert(next_capacity < static_cast<size_t>(std::numeric_limits<int>::max()));
    bytes_.CopyAndExpand(static_cast<int>(next_capacity));
    Initialize(bytes_.Data(), next_capacity);
  }

 private:
  ByteArray bytes_;
};

// TODO: Added ExportSerializeIo.h

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

    ExportSerializeInput(const void* Data, size_t length)
    {
        current_ = reinterpret_cast<const char*>(Data);
        end_ = current_ + length;
    }


    virtual ~ExportSerializeInput() {};

    inline char readChar() {
        return readPrimitive<char>();
    }

    inline int8_t readByte() {
        return readPrimitive<int8_t>();
    }

    inline int16_t readShort() {
        return readPrimitive<int16_t>();
    }

    inline int32_t readInt() {
        return readPrimitive<int32_t>();
    }

    inline bool readBool() {
        return readByte();
    }

    inline char readEnumInSingleByte() {
        return readByte();
    }

    inline int64_t readLong() {
        return readPrimitive<int64_t>();
    }

    inline float readFloat() {
        int32_t value = readPrimitive<int32_t>();
        float retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
    }

    inline double readDouble() {
        int64_t value = readPrimitive<int64_t>();
        double retval;
        memcpy(&retval, &value, sizeof(retval));
        return retval;
    }

    /** Returns a pointer to the internal Data buffer, advancing the read Position by length. */
    const void* getRawPointer(size_t length) {
        const void* result = current_;
        current_ += length;
        assert(current_ <= end_);
        return result;
    }

    /** Copy a string from the buffer. */
    inline std::string readTextString() {
        int32_t stringLength = readInt();
        assert(stringLength >= 0);
        return std::string(reinterpret_cast<const char*>(getRawPointer(stringLength)),
                stringLength);
    };

    /** Copy the next length bytes from the buffer to destination. */
    inline void readBytes(void* destination, size_t length) {
        ::memcpy(destination, getRawPointer(length), length);
    };

    /** Move the read Position back by bytes. Warning: this method is
    currently unverified and could result in reading before the
    beginning of the buffer. */
    // TODO(evanj): Change the implementation to validate this?
    void unread(size_t bytes) {
        current_ -= bytes;
    }

private:
    template <typename T>
    T readPrimitive() {
        T value;
        ::memcpy(&value, current_, sizeof(value));
        current_ += sizeof(value);
        return value;
    }

    // Current read Position.
    const char* current_;

    // End of the buffer. Valid byte range: current_ <= validPointer < end_.
    const char* end_;

    // No implicit copies
    ExportSerializeInput(const ExportSerializeInput&);
    ExportSerializeInput& operator=(const ExportSerializeInput&);
};

class ExportSerializeOutput {
  public:
    ExportSerializeOutput(void *buffer, size_t capacity) :
        buffer_(NULL), position_(0), capacity_(0)
    {
        buffer_ = reinterpret_cast<char*>(buffer);
        assert(position_ <= capacity);
        capacity_ = capacity;
    }

    virtual ~ExportSerializeOutput() {
        // the serialization wrapper never owns its Data buffer
    };

    /** Returns a pointer to the beginning of the buffer, for reading the serialized Data. */
    const char* Data() const { return buffer_; }

    /** Returns the number of bytes written in to the buffer. */
    size_t Size() const { return position_; }

    // functions for serialization
    inline void WriteChar(char value) {
        WritePrimitive(value);
    }

    inline void WriteByte(int8_t value) {
        WritePrimitive(value);
    }

    inline void WriteShort(int16_t value) {
        WritePrimitive(static_cast<uint16_t>(value));
    }

    inline void WriteInt(int32_t value) {
        WritePrimitive(value);
    }

    inline void WriteBool(bool value) {
        WriteByte(value ? int8_t(1) : int8_t(0));
    };

    inline void WriteLong(int64_t value) {
        WritePrimitive(value);
    }

    inline void WriteFloat(float value) {
        int32_t Data;
        memcpy(&Data, &value, sizeof(Data));
        WritePrimitive(Data);
    }

    inline void WriteDouble(double value) {
        int64_t Data;
        memcpy(&Data, &value, sizeof(Data));
        WritePrimitive(Data);
    }

    inline void WriteEnumInSingleByte(int value) {
        assert(std::numeric_limits<int8_t>::min() <= value &&
                value <= std::numeric_limits<int8_t>::max());
        WriteByte(static_cast<int8_t>(value));
    }

    // this explicitly accepts char* and length (or ByteArray)
    // as std::string's implicit construction is unsafe!
    inline void WriteBinaryString(const void* value, size_t length) {
        int32_t stringLength = static_cast<int32_t>(length);
        AssureExpand(length + sizeof(stringLength));

        char* current = buffer_ + position_;
        memcpy(current, &stringLength, sizeof(stringLength));
        current += sizeof(stringLength);
        memcpy(current, value, length);
        position_ += sizeof(stringLength) + length;
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

    /** Reserves length bytes of space for writing. Returns the offset to the bytes. */
    size_t ReserveBytes(size_t length) {
        AssureExpand(length);
        size_t offset = position_;
        position_ += length;
        return offset;
    }

    std::size_t Position() {
        return position_;
    }

    void Position(std::size_t pos) {
        position_ = pos;
    }

private:
    template <typename T>
    void WritePrimitive(T value) {
        AssureExpand(sizeof(value));
        memcpy(buffer_ + position_, &value, sizeof(value));
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
        assert(capacity_ >= minimum_desired);
    }

    // Beginning of the buffer.
    char* buffer_;

    // No implicit copies
    ExportSerializeOutput(const ExportSerializeOutput&);
    ExportSerializeOutput& operator=(const ExportSerializeOutput&);

protected:
    // Current Write Position in the buffer.
    size_t position_;
    // Total bytes this buffer can contain.
    size_t capacity_;
};

}  // End peloton namespace
