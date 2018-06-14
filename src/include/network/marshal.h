//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.h
//
// Identification: src/include/network/marshal.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "network/network_state.h"

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace network {

/**
 * A plain old buffer with a movable cursor, the meaning of which is dependent
 * on the use case.
 *
 * The buffer has a fix capacity and one can write a variable amount of
 * meaningful bytes into it. We call this amount "size" of the buffer.
 */
struct Buffer {
 public:
  /**
   * Instantiates a new buffer and reserve default many bytes.
   */
  inline Buffer() { buf_.reserve(SOCKET_BUFFER_SIZE); }

  /**
   * Reset the buffer pointer and clears content
   */
  inline void Reset() {
    size_ = 0;
    offset_ = 0;
  }

  /**
   * @param bytes The amount of bytes to check between the cursor and the end
   *              of the buffer (defaults to any)
   * @return Whether there is any more bytes between the cursor and
   *         the end of the buffer
   */
  inline bool HasMore(size_t bytes = 1) { return offset_ + bytes <= size_; }

  /**
   * @return Whether the buffer is at capacity. (All usable space is filled
   *          with meaningful bytes)
   */
  inline bool Full() { return size_ == Capacity(); }

  /**
   * @return Iterator to the beginning of the buffer
   */
  inline ByteBuf::const_iterator Begin() { return std::begin(buf_); }

  /**
   * @return Capacity of the buffer (not actual size)
   */
  inline size_t Capacity() const { return SOCKET_BUFFER_SIZE; }

  /**
   * Shift contents to align the current cursor with start of the buffer,
   * remove all bytes before the cursor.
   */
  inline void MoveContentToHead() {
    auto unprocessed_len = size_ - offset_;
    std::memmove(&buf_[0], &buf_[offset_], unprocessed_len);
    size_ = unprocessed_len;
    offset_ = 0;
  }

  // TODO(Tianyu): Make these protected once we refactor protocol handler
  size_t size_ = 0, offset_ = 0;
  ByteBuf buf_;
};

/**
 * A buffer specialize for read
 */
class ReadBuffer : public Buffer {
 public:
  /**
   * Read as many bytes as possible using SSL read
   * @param context SSL context to read from
   * @return the return value of ssl read
   */
  inline int FillBufferFrom(SSL *context) {
    ERR_clear_error();
    ssize_t bytes_read = SSL_read(context, &buf_[size_], Capacity() - size_);
    int err = SSL_get_error(context, bytes_read);
    if (err == SSL_ERROR_NONE) size_ += bytes_read;
    return err;
  };

  /**
   * Read as many bytes as possible using Posix from an fd
   * @param fd the file descriptor to  read from
   * @return the return value of posix read
   */
  inline int FillBufferFrom(int fd) {
    ssize_t bytes_read = read(fd, &buf_[size_], Capacity() - size_);
    if (bytes_read > 0) size_ += bytes_read;
    return (int)bytes_read;
  }

  /**
   * The number of bytes available to be consumed (i.e. meaningful bytes after
   * current read cursor)
   * @return The number of bytes available to be consumed
   */
  inline size_t BytesAvailable() { return size_ - offset_; }

  /**
   * Read the given number of bytes into destination, advancing cursor by that
   * number
   * @param bytes Number of bytes to read
   * @param dest Desired memory location to read into
   */
  inline void Read(size_t bytes, void *dest) {
    std::copy(buf_.begin() + offset_, buf_.begin() + offset_ + bytes,
              reinterpret_cast<uchar *>(dest));
    offset_ += bytes;
  }

  /**
   * Read a value of type T off of the buffer, advancing cursor by appropriate
   * amount. Does NOT convert from network bytes order. It is the caller's
   * responsibility to do so.
   * @tparam T type of value to read off. Preferably a primitive type
   * @return the value of type T
   */
  template <typename T>
  inline T ReadValue() {
    T result;
    Read(sizeof(result), &result);
    return result;
  }
};

/**
 * A buffer specialized for write
 */
class WriteBuffer : public Buffer {
 public:
  /**
   * Write as many bytes as possible using SSL write
   * @param context SSL context to write out to
   * @return return value of SSL write
   */
  inline int WriteOutTo(SSL *context) {
    ERR_clear_error();
    ssize_t bytes_written = SSL_write(context, &buf_[offset_], size_ - offset_);
    int err = SSL_get_error(context, bytes_written);
    if (err == SSL_ERROR_NONE) offset_ += bytes_written;
    return err;
  }

  /**
   * Write as many bytes as possible using Posix write to fd
   * @param fd File descriptor to write out to
   * @return return value of Posix write
   */
  inline int WriteOutTo(int fd) {
    ssize_t bytes_written = write(fd, &buf_[offset_], size_ - offset_);
    if (bytes_written > 0) offset_ += bytes_written;
    return (int)bytes_written;
  }

  /**
   * The remaining capacity of this buffer. This value is equal to the
   * maximum capacity minus the capacity already in use.
   * @return Remaining capacity
   */
  inline size_t RemainingCapacity() { return Capacity() - size_; }

  /**
   * @param bytes Desired number of bytes to write
   * @return Whether the buffer can accommodate the number of bytes given
   */
  inline bool HasSpaceFor(size_t bytes) { return RemainingCapacity() >= bytes; }

  /**
   * Append the desired range into current buffer.
   * @tparam InputIt iterator type.
   * @param first beginning of range
   * @param len length of range
   */
  template <class InputIt>
  inline void Append(InputIt first, size_t len) {
    std::copy(first, first + len, std::begin(buf_) + size_);
    size_ += len;
  }

  /**
   * Append the given value into the current buffer. Does NOT convert to
   * network byte order. It is up to the caller to do so.
   * @tparam T input type
   * @param val value to write into buffer
   */
  template <typename T>
  inline void Append(T val) {
    Append(reinterpret_cast<uchar *>(&val), sizeof(T));
  }
};

class InputPacket {
 public:
  NetworkMessageType msg_type;         // header
  size_t len;                          // size of packet without header
  size_t ptr;                          // ByteBuf cursor
  ByteBuf::const_iterator begin, end;  // start and end iterators of the buffer
  bool header_parsed;                  // has the header been parsed
  bool is_initialized;                 // has the packet been initialized
  bool is_extended;  // check if we need to use the extended buffer

 private:
  ByteBuf extended_buffer_;  // used to store packets that don't fit in rbuf

 public:
  // reserve buf's size as maximum packet size
  inline InputPacket() { Reset(); }

  // Create a packet for prepared statement parameter data before parsing it
  inline InputPacket(int len, std::string &val) {
    Reset();
    // Copy the data from string to packet buf
    this->len = len;
    extended_buffer_.resize(len);
    PELOTON_MEMCPY(extended_buffer_.data(), val.data(), len);
    InitializePacket();
  }

  inline void Reset() {
    is_initialized = header_parsed = is_extended = false;
    len = ptr = 0;
    msg_type = NetworkMessageType::NULL_COMMAND;
    extended_buffer_.clear();
  }

  inline void ReserveExtendedBuffer() {
    // grow the buffer's capacity to len
    extended_buffer_.reserve(len);
  }

  /* checks how many more bytes the extended packet requires */
  inline size_t ExtendedBytesRequired() {
    return len - extended_buffer_.size();
  }

  inline void AppendToExtendedBuffer(ByteBuf::const_iterator start,
                                     ByteBuf::const_iterator end) {
    extended_buffer_.insert(std::end(extended_buffer_), start, end);
  }

  inline void InitializePacket(size_t &pkt_start_index,
                               ByteBuf::const_iterator rbuf_begin) {
    this->begin = rbuf_begin + pkt_start_index;
    this->end = this->begin + len;
    is_initialized = true;
  }

  inline void InitializePacket() {
    this->begin = extended_buffer_.begin();
    this->end = extended_buffer_.end();
    PELOTON_ASSERT(extended_buffer_.size() == len);
    is_initialized = true;
  }

  ByteBuf::const_iterator Begin() { return begin; }

  ByteBuf::const_iterator End() { return end; }
};

struct OutputPacket {
  ByteBuf buf;                  // stores packet contents
  size_t len;                   // size of packet
  size_t ptr;                   // ByteBuf cursor, which is used for get and put
  NetworkMessageType msg_type;  // header

  bool single_type_pkt;  // there would be only a pkt type being written to the
  // buffer when this flag is true
  bool skip_header_write;  // whether we should write header to soc ket wbuf
  size_t write_ptr;        // cursor used to write packet content to socket wbuf

  // TODO could packet be reused?
  inline void Reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    single_type_pkt = false;
    len = ptr = write_ptr = 0;
    msg_type = NetworkMessageType::NULL_COMMAND;
    skip_header_write = true;
  }
};

/*
 * Marshallers
 */

/* packet_put_byte - used to write a single byte into a packet */
extern void PacketPutByte(OutputPacket *pkt, const uchar c);

/* packet_put_string - used to write a string into a packet */
extern void PacketPutStringWithTerminator(OutputPacket *pkt,
                                          const std::string &str);

/* packet_put_int - used to write a single int into a packet */
extern void PacketPutInt(OutputPacket *pkt, int n, int base);

/* packet_put_cbytes - used to write a uchar* into a packet */
extern void PacketPutCbytes(OutputPacket *pkt, const uchar *b, int len);

/* packet_put_bytes - used to write a uchar vector into a packet */
extern void PacketPutString(OutputPacket *pkt, const std::string &data);

/*
 * Unmarshallers
 */

/* Copy len bytes from the position indicated by begin to an array */
extern uchar *PacketCopyBytes(ByteBuf::const_iterator begin, int len);
/*
 * packet_get_int -  Parse an int out of the head of the
 * 	packet. "base" bytes determine the number of bytes of integer
 * 	we are parsing out.
 */
extern int PacketGetInt(InputPacket *pkt, uchar base);

/*
 * packet_get_string - parse out a string of size len.
 * 		if len=0? parse till the end of the string
 */
extern void PacketGetString(InputPacket *pkt, size_t len, std::string &result);

/* packet_get_bytes - Parse out "len" bytes of pkt as raw bytes */
extern void PacketGetBytes(InputPacket *pkt, size_t len, ByteBuf &result);

/* packet_get_byte - Parse out a single bytes from pkt */
extern void PacketGetByte(InputPacket *rpkt, uchar &result);

/*
 * get_string_token - used to extract a string token
 * 		from an unsigned char vector
 */
extern void GetStringToken(InputPacket *pkt, std::string &result);

}  // namespace network
}  // namespace peloton
