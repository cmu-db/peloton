//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.h
//
// Identification: src/include/wire/marshal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

#include "common/logger.h"
#include "common/types.h"

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace wire {

class LibeventSocket;

struct InputPacket {
  uchar msg_type;   // header
  size_t len;       // size of packet
  size_t ptr;       // PktBuf cursor
  SockBuf::const_iterator begin, end; // start and end iterators of the buffer
  bool header_parsed;   // has the header been parsed
  bool is_initialized;  // has the packet been initialized

  // reserve buf's size as maximum packet size
  inline InputPacket() { Reset(); }

  inline void Reset() {
    is_initialized = header_parsed = false;
    len = ptr = msg_type = 0;
  }

  inline void InitializePacket(size_t &pkt_start_index,
                               SockBuf::const_iterator rbuf_begin) {
    this->begin = rbuf_begin + pkt_start_index;
    this->end = rbuf_begin + len;
    is_initialized = true;
  }

  SockBuf::const_iterator Begin() {
    return begin;
  }

  SockBuf::const_iterator End() {
    return end;
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

///* packet_put_byte - used to write a single byte into a packet */
//extern void PacketPutByte(std::unique_ptr<Packet> &pkt, const uchar c);
//
///* packet_put_string - used to write a string into a packet */
//extern void PacketPutString(std::unique_ptr<Packet> &pkt,
//                            const std::string &str);
//
///* packet_put_int - used to write a single int into a packet */
//extern void PacketPutInt(std::unique_ptr<Packet> &pkt, int n, int base);
//
///* packet_put_cbytes - used to write a uchar* into a packet */
//extern void PacketPutCbytes(std::unique_ptr<Packet> &pkt, const uchar *b,
//                            int len);
//
///* packet_put_bytes - used to write a uchar vector into a packet */
//extern void PacketPutBytes(std::unique_ptr<Packet> &pkt,
//                           const std::vector<uchar> &data);

/*
 * Unmarshallers
 */

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
extern void PacketGetBytes(InputPacket *pkt, size_t len, PktBuf &result);

/*
 * get_string_token - used to extract a string token
 * 		from an unsigned char vector
 */
extern void GetStringToken(InputPacket *pkt, std::string &result);

/*
 * Socket layer interface - Link the protocol to the socket buffers
 */

///* Read a single packet from the socket read buffer */
//extern bool ReadPacket(Packet *pkt, bool has_type_field, Client *client);

}  // End wire namespace
}  // End peloton namespace
