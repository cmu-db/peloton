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

struct Packet {
  PktBuf buf;      // stores packet contents
  size_t len;      // size of packet
  size_t ptr;      // PktBuf cursor, which is used for get and put
  uchar msg_type;  // header

  bool skip_header_write;  // whether we should write header to socket wbuf
  size_t write_ptr;        // cursor used to write packet content to socket wbuf

  // reserve buf's size as maximum packet size
  inline Packet() { Reset(); }

  // TODO could packet be reused?
  inline void Reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    len = ptr = write_ptr = msg_type = 0;
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
extern void PacketPutByte(std::unique_ptr<Packet> &pkt, const uchar c);

/* packet_put_string - used to write a string into a packet */
extern void PacketPutString(std::unique_ptr<Packet> &pkt,
                            const std::string &str);

/* packet_put_int - used to write a single int into a packet */
extern void PacketPutInt(std::unique_ptr<Packet> &pkt, int n, int base);

/* packet_put_cbytes - used to write a uchar* into a packet */
extern void PacketPutCbytes(std::unique_ptr<Packet> &pkt, const uchar *b,
                            int len);

/* packet_put_bytes - used to write a uchar vector into a packet */
extern void PacketPutBytes(std::unique_ptr<Packet> &pkt,
                           const std::vector<uchar> &data);

/*
 * Unmarshallers
 */

/*
 * packet_get_int -  Parse an int out of the head of the
 * 	packet. "base" bytes determine the number of bytes of integer
 * 	we are parsing out.
 */
extern int PacketGetInt(Packet *pkt, uchar base);

/*
 * packet_get_string - parse out a string of size len.
 * 		if len=0? parse till the end of the string
 */
extern void PacketGetString(Packet *pkt, size_t len, std::string &result);

/* packet_get_bytes - Parse out "len" bytes of pkt as raw bytes */
extern void PacketGetBytes(Packet *pkt, size_t len, PktBuf &result);

/*
 * get_string_token - used to extract a string token
 * 		from an unsigned char vector
 */
extern void GetStringToken(Packet *pkt, std::string &result);

/*
 * Socket layer interface - Link the protocol to the socket buffers
 */

///* Read a single packet from the socket read buffer */
//extern bool ReadPacket(Packet *pkt, bool has_type_field, Client *client);

}  // End wire namespace
}  // End peloton namespace
