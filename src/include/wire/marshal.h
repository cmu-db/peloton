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

#include "wire/socket_base.h"
#include "wire/wire.h"
#include "common/logger.h"

namespace peloton {
namespace wire {

typedef unsigned char uchar;

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

/* Write a batch of packets to the socket write buffer */
extern bool WritePackets(std::vector<std::unique_ptr<Packet>> &packets,
                         Client *client);

/* Read a single packet from the socket read buffer */
extern bool ReadPacket(Packet *pkt, bool has_type_field, Client *client);

}  // End wire namespace
}  // End peloton namespace
