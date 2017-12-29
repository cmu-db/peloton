//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.h
//
// Identification: src/include/network/marshal.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"
#include "common/internal_types.h"

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace network {

// Buffers used to batch messages at the socket
struct Buffer {
  size_t buf_ptr;        // buffer cursor
  size_t buf_size;       // buffer size
  size_t buf_flush_ptr;  // buffer cursor for write
  ByteBuf buf;

  inline Buffer() : buf_ptr(0), buf_size(0), buf_flush_ptr(0) {
    // capacity of the buffer
    buf.reserve(SOCKET_BUFFER_SIZE);
  }

  inline void Reset() {
    buf_ptr = 0;
    buf_size = 0;
    buf_flush_ptr = 0;
  }

  // single buffer element accessor
  inline uchar GetByte(size_t &index) { return buf[index]; }

  // Get pointer to index location
  inline uchar *GetPtr(size_t index) { return &buf[index]; }

  inline ByteBuf::const_iterator Begin() { return std::begin(buf); }

  inline ByteBuf::const_iterator End() { return std::end(buf); }

  inline size_t GetMaxSize() { return SOCKET_BUFFER_SIZE; }

  //Get the 4 bytes Big endian uint32 and convert it to little endian
  size_t GetUInt32BigEndian();

  // Is the requested amount of data available from the current position in
  // the reader buffer?
  inline bool IsReadDataAvailable(size_t bytes) {
    return ((buf_ptr - 1) + bytes < buf_size);
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
    PL_MEMCPY(extended_buffer_.data(), val.data(), len);
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
    PL_ASSERT(extended_buffer_.size() == len);
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

  bool skip_header_write;  // whether we should write header to socket wbuf
  size_t write_ptr;        // cursor used to write packet content to socket wbuf

  // TODO could packet be reused?
  inline void Reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    len = ptr = write_ptr = 0;
    msg_type = NetworkMessageType::NULL_COMMAND;
    skip_header_write = true;
  }
};

struct Client {
  // Authentication details
  std::string dbname;
  std::string user;
  std::unordered_map<std::string, std::string> cmdline_options;

  inline void Reset() {
    dbname.clear();
    user.clear();
    cmdline_options.clear();
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
